-module(c_agent_client).
-export([start/0, armar_lista/1, get_nodes/1]).
-export([masterloop/3, disparar_rafaga/4, armar_comando/1, job_handler/1, host_sort/1]).
-export([node_suffix/0, nuevo_job_id/0]).
-export([test_deadlock1/0, test_deadlock2/0]).

-define(PORT, 8000).
-define(HOST, "localhost").
-define(GET_NODES, "GET_NODES\n").

-define(RAFAGA, 2).
-define(CANT_NODOS, 4).
-define(DIVISOR, 16).

% Genera el sufijo del Job_id basándose en la IP
node_suffix() ->
    case get(node_suffix) of
        undefined ->
            Suffix = calcular_node_suffix(),
            put(node_suffix, Suffix),
            Suffix;
        Valor -> Valor
    end.

% Función auxiliar de node_suffix
calcular_node_suffix() ->
    Bruto = 
        case os:getenv("NODE_SUFFIX") of
        false ->
            case inet:getif() of
                {ok, Ifs} ->
                    % En la lista de tuplas {IP, BCast, Mask}, 
                    % conserva sólo las IP que no sean loopback
                    NoLoopback = [Ip || {Ip, _Bcast, _Mask} <- Ifs, Ip =/= {127, 0, 0, 1}],
                    case NoLoopback of
                        % Agarra el último octeto de la primera IP
                        % o genera un número aleatorio hasta 255
                        [{_, _, _, Ultimo} | _] -> Ultimo;
                        [] -> rand:uniform(256) - 1
                    end;
                _ -> rand:uniform(256) - 1
            end;
        Valor -> list_to_integer(Valor)
    end,
    % Retorna el resto de Bruto/256
    Bruto rem 256.

% Genera un job_id único
nuevo_job_id() ->
    erlang:unique_integer([positive]) * 256 + node_suffix().

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
    Max = max(1, N div ?DIVISOR), 
    rand:uniform(Max + 1) - 1.

% Ordena los nodos según el valor de "host" (Estrategia anti-deadlock) 
host_sort(Maps_list) ->
    lists:sort(
        fun(A, B) -> maps:get("host", A) =< maps:get("host", B) end, 
        Maps_list
    ).

% Elige N elementos aleatorios de la lista dada
elegir_n_aleatorios(Lista, N) ->
    ConClaveRandom = [{rand:uniform(), X} || X <- Lista],
    Ordenada = lists:sort(ConClaveRandom),
    Primeros = lists:sublist(Ordenada, N),
    [X || {_, X} <- Primeros].

% Primera parte de "armar_comando"
% Ordena la lista de nodos y arma el comando de JOB_REQUEST 
% que se enviará al agente c 
armar_comando(Maps_list) ->
    NodosElegidos = elegir_n_aleatorios(Maps_list, ?CANT_NODOS),
    ListaOrdenada = host_sort(NodosElegidos),
    armar_comando_ordenado(ListaOrdenada).

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
job_handler(Job_id) ->
    io:format("Job ~p esperando asignación~n", [Job_id]),
    receive
        granted ->
            io:format("Job ~p trabajando...~n", [Job_id]),
            sleep(1000 + rand:uniform(4000)),
            io:format("Job ~p finalizó. Solicitando RELEASE...~n", [Job_id]);
        denied ->
            io:format("Job ~p DENEGADO. Abortando...~n", [Job_id]);
        timeout ->
            io:format("Job ~p (Timeout). Abortando...~n", [Job_id])
        after 30000 ->
            % Paso algo raro, demasiado tiempo esperando
            registrar_log(Job_id, "EXPIRADO (LOCAL)", "Tiempo de espera agotado."),
            io:format("Job ~p (Timeout). Abortando...~n", [Job_id])
    end,
    
    % Si no existe el proceso master, no se hace el send
    % El agente C ya libera los recursos por su cuenta
    case whereis(master) of
        undefined -> nothing;
        _ -> master ! {release, Job_id}
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
                    Job_id = nuevo_job_id(),
                    gen_tcp:send(Socket, "JOB_REQUEST " ++ integer_to_list(Job_id) ++ " " ++ Comando ++ "\n"),

                    Pid = spawn(?MODULE, job_handler, [Job_id]),
                    NewMap = maps:put(Job_id, Pid, HandlersMap),
                    disparar_rafaga(Maps_list, Socket, Cantidad - 1, NewMap)
            end
    end.

% Función para registrar eventos en un archivo físico de logs con estampa de tiempo
registrar_log(JobId, Estado, Detalle) ->
    {_, {H, Min, S}} = erlang:localtime(),
    LogTexto = io_lib:format("[~.2.0w:~.2.0w:~.2.0w] [JOB ~p] [~s] -> ~s~n", [H, Min, S, JobId, Estado, Detalle]),
    % Abre el archivo en modo append (escribir al final)
    file:write_file("planificador.log", LogTexto, [append]).

% Envía GET_NODES al agente C y recibe la lista de nodos
get_nodes(Socket) ->
    gen_tcp:send(Socket, ?GET_NODES),
    get_nodes_recv(Socket).

% Si se recibe un mensaje que no sea el de los nodos,
% se evita enviar el GET_NODES otra vez
get_nodes_recv(Socket) ->
    case gen_tcp:recv(Socket, 0) of
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

        % Si el cliente aborta el job por timeout justo antes de recibir respuesta
        % del agente C, puede que la recibamos estando acá. Ignoramos el mensaje
        {ok, Mensaje} ->
            io:format("Mensaje tardío ignorado: ~p ~n", [Mensaje]),
            get_nodes_recv(Socket);

        {error, Reason} ->
            io:format("Error: No se pudo recibir la información sobre los nodos: ~p ~n", [Reason]),
            []
    end.

% Loop principal del proceso master
masterloop(Maps_list, Socket, HandlersMap) ->
    
    % Si el mapa se vació, significa que la ráfaga anterior terminó.
    % Actualizamos la lista de nodos    
    NuevoMapsList = 
        case maps:size(HandlersMap) of
            0 -> get_nodes(Socket);
            _ -> Maps_list
        end,

    case NuevoMapsList of
        
        % Si la lista está vacía, estamos en un caso de error
        % o de ausencia de nodos. Terminamos el programa
        [] -> io:format("Cerrando conexión y terminando programa...~n");

        _ ->
            % Si la lista no está vacía, mandamos otra ráfaga
            MapConJobs = 
                case maps:size(HandlersMap) of
                    0 -> 
                        sleep(1000 + rand:uniform(2000)), % Pausa para no saturar al Agente
                        disparar_rafaga(NuevoMapsList, Socket, ?RAFAGA, HandlersMap);
                    _ -> 
                        HandlersMap
                end,

            % Primero vaciamos el buzón de mensajes internos de Erlang
            % Esto espera de cualquier proceso llamado con job_handler
            receive
                {release, JobIdToRelease} ->
                    gen_tcp:send(Socket, "JOB_RELEASE " ++ integer_to_list(JobIdToRelease) ++ "\n"),
                    registrar_log(JobIdToRelease, "RELEASE", "Liberación de recursos solicitada."),
                    % Sacamos el Job del mapa para que maps:size() eventualmente llegue a cero
                    CleanMapInterno = maps:remove(JobIdToRelease, MapConJobs),
                    masterloop(NuevoMapsList, Socket, CleanMapInterno)
            after 0 -> 
                % Si no hay liberaciones internas, escuchamos lo que llega de la red (Agente C)
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
            % Y una lista de nodos, ambos vacíos
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

            % Armamos el string a partir del orden ya decidido por la función
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

            % Armamos el string a partir del orden ya decidido por la función
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
