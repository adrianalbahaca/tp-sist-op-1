-module(c_agent_client).
-export([start/0, armar_lista/1, get_nodes/1]).
-export([masterloop/3, disparar_rafaga/4, armar_comando/1, job_handler/2, host_sort/1]).
-export([test_deadlock1/0, test_deadlock2/0]).

-define(PORT, 8000).
-define(HOST, "localhost").
-define(GET_NODES, "GET_NODES\n").

-define(RAFAGA, 3).

% Arma la lista de pares que luego se utilizará para armar el mapa del nodo correspondiente
armar_lista(Node_listed) -> 
    case Node_listed of
        [] -> [];
        ["host", Second, _Third | Tail] -> [{"host", Second}] ++ armar_lista(Tail);
        ["gpu", Second | Tail] -> [{"gpu", string:trim(Second)}] ++ armar_lista(Tail);
        [First, Second | Tail] -> [{First, Second}] ++ armar_lista(Tail)
    end.

% Similar a rand:uniform, pero desde 0 a N/3
rand_desde_cero(0) -> 0;
rand_desde_cero(N) -> 
    Max = max(1, N div 5), 
    rand:uniform(Max + 1) - 1.

% Ordena los nodos según el valor de "host" (Estrategia anti-deadlock) 
host_sort(Maps_list) ->
    lists:sort(
        fun(A, B) -> maps:get("host", A) =< maps:get("host", B) end, 
        Maps_list
    ).

armar_comando(Maps_list) -> 
    ListaOrdenada = lists:sort(
        fun(A, B) -> maps:get("host", A) =< maps:get("host", B) end, 
        Maps_list
    ),
    armar_comando_ordenado(ListaOrdenada).
    
    %armar_comando_ordenado([lists:nth(rand:uniform(length(Maps_list)), Maps_list)]).

% Primera parte de "armar_comando"
% Ordena la lista de nodos y arma el comando de JOB_REQUEST 
% que se enviará al agente c 

% Segunda parte de "armar_comando"
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
                    "@" ++ maps:get("host", Head) ++ 
                    case First of
                        0 -> "";
                        First -> ":cpu:" ++ integer_to_list(First) 
                    end ++ 
                    
                    case Second of
                        0 -> "";
                        Second -> ":mem:" ++ integer_to_list(Second) 
                    end ++ 
                    
                    case Third of
                        0 -> "";
                        Third -> ":gpu:" ++ integer_to_list(Third) 
                    end ++
                 
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
job_handler(Job_id, _GrantsEsperados) ->
        receive
            granted ->
                io:format("Job ~p trabajando...~n", [Job_id]),
                sleep(1000 + rand:uniform(4000)),
                io:format("Job ~p finalizó. Solicitando RELEASE...~n", [Job_id]),
                master ! {release, Job_id};
            denied ->
                io:format("Job ~p DENEGADO. Abortando...~n", [Job_id]),
                master ! {release, Job_id}
            after 30000 ->
                % Paso algo raro, demasiado tiempo esperando
                io:format("Job ~p (Timeout). Abortando...~n", [Job_id]),
                master ! {release, Job_id}
        end.

% Dispara los 4 requests seguidos al socket en paralelo
% También arma el map con los jobs pendientes
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
                    
                    %io:format(user, "JOB_REQUEST ~p ~s~n", [Job_id, Comando]),
                    % Se calcula la cantidad de nodos (IPs) involucrados contando el delimitador '@'
                    % Tokens = string:split(string:trim(Comando), "@", all),
                    % GrantsEsperados = length(Tokens) - 1,

                    Pid = spawn(?MODULE, job_handler, [Job_id, 1]),
                    NewMap = maps:put(Job_id, Pid, HandlersMap),
                    disparar_rafaga(Maps_list, Socket, Cantidad - 1, NewMap)
            end
    end.

% Función para registrar eventos en un archivo físico de logs con estampa de tiempo
registrar_log(JobId, Estado, Detalle) ->
    {_, {H, Min, S}} = erlang:localtime(),
    LogTexto = io_lib:format("[~.2.0w:~.2.0w:~.2.0w] [JOB ~p] [~s] -> ~s~n", [H, Min, S, JobId, Estado, Detalle]),
    % Abre el archivo en modo append (agregar al final) y escribe de forma segura
    file:write_file("planificador.log", LogTexto, [append]).

get_nodes(Socket) ->
    gen_tcp:send(Socket, ?GET_NODES),
    %io:format("Mensaje enviado: ~s", [?GET_NODES]),
    
    case gen_tcp:recv(Socket, 0, 5000) of
        {ok, "NODES " ++ Data} ->
            Data_listed = string:split(string:trim(Data), ";", all), 
            
            % A cada elemento de Data_Listed, le hacemos split por ":",
            % le ponemos "host" al inicio, le aplicamos armar_lista y
            % la convertimos en un mapa
            ListDeMapas = [
                maps:from_list(armar_lista(["host" | string:split(Nodo, ":", all)]))
                || Nodo <- Data_listed, Nodo /= ""
            ],
            
            ListDeMapas;

        {error, Reason} ->
            io:format("Error: No se pudo recibir la información sobre los nodos: ~p ~n", [Reason]),
            gen_tcp:close(Socket),
            []
    end.

% Loop principal del proceso master
masterloop(Maps_list, Socket, HandlersMap) ->
    % Si el mapa se vació, significa que la ráfaga anterior terminó con éxito. Disparamos otra.
    
    % Si terminó la ráfaga, actualizamos la lista de nodos    
    NuevoMapsList = 
        case maps:size(HandlersMap) of
            0 -> get_nodes(Socket);
            _ -> Maps_list
        end,

    % Y luego, mandamos otra
    MapConJobs = 
        case maps:size(HandlersMap) of
            0 -> 
                sleep(1000 + rand:uniform(2000)), % Pausa para no saturar al Agente
                disparar_rafaga(NuevoMapsList, Socket, ?RAFAGA, HandlersMap);
            _ -> 
                HandlersMap
        end,

    % Primero vaciamos el buzón de mensajes internos de Erlang
    receive
        {release, JobIdToRelease} ->
            gen_tcp:send(Socket, "JOB_RELEASE " ++ integer_to_list(JobIdToRelease) ++ "\n"),
            % Sacamos el Job del mapa para que maps:size() eventualmente llegue a cero
            CleanMapInterno = maps:remove(JobIdToRelease, MapConJobs),
            masterloop(NuevoMapsList, Socket, CleanMapInterno)
    after 0 -> 
        % Si no hay liberaciones internas, escuchamos lo que llega de la red (Agente C)
        %io:format("ESPERANDO~n"),
        case gen_tcp:recv(Socket, 0, 5000) of
            {ok, LineaConSalto} ->
                % Removemos el \n de la línea
                Linea = string:trim(LineaConSalto, trailing, "\n"),
                
                {TipoMensaje, IdCrudo} = 
                    case Linea of
                        "JOB_GRANTED " ++ Resto -> {granted, Resto};
                        "JOB_DENIED " ++ Resto  -> {denied, Resto};
                        "JOB_TIMEOUT " ++ Resto -> {timeout, Resto};
                        _                       -> {desconocido, ""}
                    end,

                case TipoMensaje of
                    desconocido ->
                        masterloop(NuevoMapsList, Socket, MapConJobs);
                    _ ->
                        JobIdRecibido = list_to_integer(string:trim(IdCrudo)),
                        case maps:find(JobIdRecibido, MapConJobs) of
                            {ok, Destinatario} ->
                                Destinatario ! TipoMensaje,

                                % Sección de registro en archivo log
                                case TipoMensaje of
                                    granted -> registrar_log(JobIdRecibido, "OTORGADO", "Recursos reservados correctamente.");
                                    denied  -> registrar_log(JobIdRecibido, "DENEGADO", "Fallo en la reserva. Nodos saturados.");
                                    timeout -> registrar_log(JobIdRecibido, "EXPIRADO", "Tiempo de espera agotado.")
                                end,

                                masterloop(NuevoMapsList, Socket, MapConJobs);
                            error ->
                                masterloop(NuevoMapsList, Socket, MapConJobs)
                        end
                end;

            {error, timeout} ->
                masterloop(NuevoMapsList, Socket, MapConJobs);
            
            {error, Reason} ->
                io:format("Error crítico con el Agente C: ~p~n", [Reason])
        end
    end.
% Función para iniciar el cliente de Erlang
start() ->
    rand:seed(exsp), % Similar a srand(time(NULL)) en C

    % Nos conectamos al agente C
    case gen_tcp:connect(?HOST, ?PORT, [list, {packet, line}, {active, false}]) of
        {ok, Socket} ->
            io:format("Conectado al agente C en el puerto ~p~n", [?PORT]),
            register(master, self()),
    
            % Arrancamos el masterloop con un mapa de jobs pendientes
            % Y uno de nodos, ambos vacíos #{}
            io:format("Entrando a masterloop...~n"),
            masterloop([], Socket, #{}),
            gen_tcp:close(Socket);
            
            
        {error, Reason} ->
            io:format("Conexión fallida: ~p~n", [Reason])
    end.

% Función para el test de deadlock (NODO A)
test_deadlock1() ->
    case gen_tcp:connect(?HOST, ?PORT, [list, {packet, line}, {active, false}]) of
    
        {ok, Socket} ->

            NodoAzul = maps:from_list([{"host", "10.0.0.10"}, {"recurso", "cpu:2"}]),
            NodoRojo = maps:from_list([{"host", "10.0.0.20"}, {"recurso", "gpu:1"}]),
            Ordenados = host_sort([NodoAzul, NodoRojo]), % Pedir CPU primero

            % Armamos el string a partir del orden YA decidido por la función
            Comando = lists:foldl(
                fun(Map, Acc) -> Acc ++ "@" ++ maps:get("host", Map) ++ ":" ++ maps:get("recurso", Map) ++ " " end,
                "",
                Ordenados
            ),

            Job_id = 1001,
            Mensaje = "JOB_REQUEST " ++ integer_to_list(Job_id) ++ " " ++ string:trim(Comando) ++ "\n",
            io:fwrite("Cliente A envía: " ++ Mensaje),
            gen_tcp:send(Socket, Mensaje),

            case gen_tcp:recv(Socket, 0, 30000) of
                {ok, "JOB_GRANTED " ++ _Resto} ->
                    io:fwrite("Job de A está trabajando...~n"),
                    sleep(2000),
                    gen_tcp:send(Socket, "JOB_RELEASE " ++ integer_to_list(Job_id) ++ "\n");
                
                {ok, "JOB_DENIED " ++ _Job_id} -> io:fwrite("El Job de A fue denegado: ~n");
                {ok, "JOB_TIMEOUT " ++ _Job_id} -> io:fwrite("El Job de A expiró: ~n");
                {error, Reason} -> io:fwrite("Error de recepción: ~p~n", [Reason])
            end,
            gen_tcp:close(Socket);

        {error, Reason} -> io:format("Conexión fallida: ~p~n", [Reason])
    end.

% Función para el test de deadlock (NODO B)
test_deadlock2() ->
    case gen_tcp:connect(?HOST, ?PORT, [list, {packet, line}, {active, false}]) of
    
        {ok, Socket} ->

            NodoAzul = maps:from_list([{"host", "10.0.0.10"}, {"recurso", "cpu:2"}]),
            NodoRojo = maps:from_list([{"host", "10.0.0.20"}, {"recurso", "gpu:1"}]),
            Ordenados = host_sort([NodoRojo, NodoAzul]), % Pedir GPU primero

            % Armamos el string a partir del orden YA decidido por la función
            Comando = lists:foldl(
                fun(Map, Acc) -> Acc ++ "@" ++ maps:get("host", Map) ++ ":" ++ maps:get("recurso", Map) ++ " " end,
                "",
                Ordenados
            ),

            Job_id = 1002,
            Mensaje = "JOB_REQUEST " ++ integer_to_list(Job_id) ++ " " ++ string:trim(Comando) ++ "\n",
            io:fwrite("Cliente B envía: " ++ Mensaje), % Se reordenó el comando
            gen_tcp:send(Socket, Mensaje),

            case gen_tcp:recv(Socket, 0, 30000) of
                {ok, "JOB_GRANTED " ++ _Resto} ->
                    io:fwrite("Job de B está trabajando...~n"),
                    sleep(2000),
                    gen_tcp:send(Socket, "JOB_RELEASE " ++ integer_to_list(Job_id) ++ "\n");
                
                {ok, "JOB_DENIED " ++ _Job_id} -> io:fwrite("El Job de B fue denegado: ~n");
                {ok, "JOB_TIMEOUT " ++ _Job_id} -> io:fwrite("El Job de B expiró: ~n");
                {error, Reason} -> io:fwrite("Error de recepción: ~p~n", [Reason])
            end,

            gen_tcp:close(Socket);

        {error, Reason} -> io:format("Conexión fallida: ~p~n", [Reason])
    end.
