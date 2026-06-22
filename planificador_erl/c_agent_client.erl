-module(c_agent_client).
-export([start/0, map_gen/1, node_map/1, armar_lista/1]).
-export([get_list_maps/1, masterloop/3, disparar_rafaga/4, armar_comando/1, job_handler/2]).

-define(PORT, 8000).
-define(HOST, "localhost").
-define(GET_NODES, "GET_NODES\n").

-define(RAFAGA, 4).

% Arma la lista de pares que luego se utilizará para armar el mapa
armar_lista(Node_listed) -> 
    case Node_listed of
        [] -> [];
        ["host", Second, _Third | Tail] -> [{"host", Second}] ++ armar_lista(Tail);
        [First, Second | Tail] -> [{First, Second}] ++ armar_lista(Tail)
    end.

% Función auxiliar de armado de mapas de nodos
node_map(Node_listed) ->
    Node_paired = armar_lista(Node_listed),
    Map = maps:from_list(Node_paired),
    master ! Map,
    map_sent_master.

% Genera los mapas que representan los nodos
map_gen(Nodes) ->
    case Nodes of
        [] -> generation_done;
        [Head | Tail] -> 
            Head_listed = string:split(Head, ":", all),
            spawn(?MODULE, node_map, [["host"] ++ Head_listed]),
            map_gen(Tail)
    end.

% Recolector síncrono estricto por conteo de nodos
get_list_maps(N) ->
    case N of
        0 -> [];
        N ->
            receive
                Mapa -> [Mapa | get_list_maps(N - 1)]
            end
    end.

% Similar a rand:uniform, pero desde 0 a N
rand_desde_cero(N) -> 
    case N of
        0 -> 0;
        N -> rand:uniform(N + 1) - 1
    end.

% Ordena los nodos según el valor de "host" (Estrategia anti-deadlock)
armar_comando(Maps_list) ->
    ListaOrdenada = lists:sort(
        fun(A, B) -> maps:get("host", A) =< maps:get("host", B) end, 
        Maps_list
    ),
    armar_comando_ordenado(ListaOrdenada).

% Arma el comando una vez ordenada la lista de nodos
armar_comando_ordenado(Maps_list) ->
    case Maps_list of
        [] -> "";
        [Head | Tail] ->
            Rand_list = [rand_desde_cero(list_to_integer(maps:get("cpu", Head, "0"))),
                         rand_desde_cero(list_to_integer(maps:get("mem", Head, "0"))),
                         rand_desde_cero(list_to_integer(maps:get("gpu", Head, "0")))],

            case Rand_list of
                [0, 0, 0] -> "" ++ armar_comando_ordenado(Tail);
                [First, Second, Third] -> 
                    "@" ++ maps:get("host", Head) ++ ":cpu:" ++ integer_to_list(First) ++ 
                    ":mem:" ++ integer_to_list(Second) ++ ":gpu:" ++ integer_to_list(Third) ++ 
                    case Tail of 
                        [] -> ""; 
                        _ -> " " 
                    end ++ armar_comando_ordenado(Tail)
            end
    end.

% Suspende la ejecución del programa por Ms milisegundos
sleep(Ms) ->
    receive 
        after 
            Ms -> go_on 
    end.

% Vigila el estado del job recibido como argumento
job_handler(Job_id, Socket) ->
    receive
        granted ->
            io:format("Job ~p trabajando...~n", [Job_id]),
            sleep(5000),
            gen_tcp:send(Socket, "JOB_RELEASE " ++ integer_to_list(Job_id) ++ "\n"),
            io:format("Job ~p envió RELEASE~n", [Job_id]);
        denied ->
            io:format("Job ~p denegado~n", [Job_id]);
        timeout ->
            io:format("Job ~p expiró por timeout~n", [Job_id])
    end.

% Dispara los 4 requests seguidos al socket en paralelo
% También arma el map con los jobs solicitados sin respuesta del agente C
disparar_rafaga(Maps_list, Socket, Cantidad, HandlersMap) ->
    case Cantidad of

        0 -> HandlersMap;

        Cantidad ->
            Comando = armar_comando(Maps_list),
            case Comando of
                "" -> 
                    disparar_rafaga(Maps_list, Socket, Cantidad - 1, HandlersMap);
                _ ->
                    Job_id = erlang:unique_integer([positive]), 
                    gen_tcp:send(Socket, "JOB_REQUEST " ++ integer_to_list(Job_id) ++ " " ++ Comando ++ "\n"),
                    Pid = spawn(?MODULE, job_handler, [Job_id, Socket]),
                    NewMap = maps:put(Job_id, Pid, HandlersMap),
                    disparar_rafaga(Maps_list, Socket, Cantidad - 1, NewMap)
            end
    end.

% Loop principal del proceso master
masterloop(Maps_list, Socket, HandlersMap) ->
    % Si el mapa se vació, significa que la ráfaga anterior terminó. Disparamos otra.
    MapConJobs = 
        case maps:size(HandlersMap) of
            0 -> 
                sleep(2000), % Pausa para no saturar al Agente
                disparar_rafaga(Maps_list, Socket, ?RAFAGA, HandlersMap);
            _ -> 
                HandlersMap
        end,

    % Recibimos respuesta al JOB_REQUEST y actuamos al respecto
    case gen_tcp:recv(Socket, 0, 500) of
        {ok, Linea} ->
            {TipoMensaje, IdCrudo} = 
                case Linea of
                "JOB_GRANTED " ++ Resto -> {granted, Resto};
                "JOB_DENIED " ++ Resto  -> {denied, Resto};
                "JOB_TIMEOUT " ++ Resto -> {timeout, Resto};
                _                       -> {desconocido, ""}
                end,

            case TipoMensaje of
                desconocido ->
                    masterloop(Maps_list, Socket, MapConJobs);
                _ ->
                    JobIdRecibido = list_to_integer(string:trim(IdCrudo)),
                    case maps:find(JobIdRecibido, MapConJobs) of
                        {ok, Destinatario} ->
                            Destinatario ! TipoMensaje,
                            CleanMap = maps:remove(JobIdRecibido, MapConJobs),
                            masterloop(Maps_list, Socket, CleanMap);
                        error ->
                            masterloop(Maps_list, Socket, MapConJobs)
                    end
            end;

        {error, timeout} ->
            masterloop(Maps_list, Socket, MapConJobs);
        
        % Solo terminamos 
        {error, Reason} ->
            io:format("Error con el Agente C: ~p~n", [Reason])
    end.

% Función para iniciar el cliente de Erlang
start() ->
    rand:seed(exsp), % Similar a srand(time(NULL)) en C

    % Nos conectamos al agente C
    case gen_tcp:connect(?HOST, ?PORT, [list, {packet, 0}, {active, false}]) of
        {ok, Socket} ->
            io:format("Conectado al agente C en el puerto ~p~n", [?PORT]),
            register(master, self()),
            
            % Solicitamos la información sobre nodos
            gen_tcp:send(Socket, ?GET_NODES),
            io:format("Mensaje enviado: ~s", [?GET_NODES]),
            
            % La recibimos con un tiempo de espera máximo de 5 segundos
            case gen_tcp:recv(Socket, 0, 5000) of
                {ok, "NODES " ++ Data} ->
                    Data_listed = string:split(Data, ";", all), 
                    map_gen(Data_listed), 
                    
                    % Armamos la lista de mapas para más comodidad en el manejo de datos
                    % Cada mapa representa un nodo
                    Maps_list = get_list_maps(length(Data_listed)), 
                    io:format("Entrando a serverloop...~n"),
                    
                    % Arrancamos el serverloop con un mapa de jobs pendientes vacío #{}
                    masterloop(Maps_list, Socket, #{}),
                    gen_tcp:close(Socket);
                
                _ ->
                    io:format("Error: No se pudo recibir la información sobre los nodos~n"),
                    gen_tcp:close(Socket)
            end;
        {error, Reason} ->
            io:format("Conexión fallida: ~p~n", [Reason])
    end.