-module(c_agent_client).
-export([start/0, map_gen/1, node_map/1, armar_lista/1]).
-export([get_list_maps/0, serverloop/3, armar_comando/1, job_handler/1]).

-define(PORT, 8000).
-define(HOST, "localhost").
-define(GET_NODES, <<"GET_NODES">>).

-define(RAFAGA, 4).

% 1.22.312:1212:cpu:1:mem:2212:gpu:1
% @ip
armar_lista(Node_listed) -> 
    case Node_listed of
        [] -> [];
        ["host", Second, Third | Tail] -> [{"host", Second}] ++ armar_lista(Tail);
        [First, Second | Tail] -> [{First, Second}] ++ armar_lista(Tail)
    end. % [{"host", <ip>}, {"cpu", <n_cpu>}, ...]

% Función auxiliar de generación de mapas
node_map(Node_listed) ->
    Node_paired = armar_lista(Node_listed),
    Map = maps:from_list(Node_paired),
    master ! Map,
    done.

% Generador de mapas para saber más facilmente
% los recursos disponibles
% Se generan a partir de una lista de nodos
map_gen(Nodes) -> %hacer secuencial
    case Nodes of
        
        [] -> generation_done;
        
        [Head | Tail] -> 
            Head_listed = string:split(Head, <<":">>, all),
            spawn(?MODULE, node_map, [["host"] ++ Head_listed]), % ["host", <ip>, <puerto>, "cpu", <n_cpu>, "mem", <mb_mem>, "gpu", <n_gpu>]
            map_gen(Tail)

    end.

% Arma la lista de mapas, donde cada uno representa un nodo
get_list_maps() ->
    
    io:fwrite("Acomodando datos, espere...~n"), %%%
    receive
        Mapa -> [Mapa] ++ get_list_maps()

        after
        5000 -> maps_are_here
    end.

% Genera un número aleatorio desde 0 a N inclusive
rand_desde_cero(N) ->
    case N of
        0 -> 0;
        N -> rand:uniform(N + 1) - 1
    end.

% Construye parte del comando de JOB_REQUEST que se enviará al agente C
armar_comando(Maps_list) ->
    
    case Maps_list of
        
        [Head] -> Rand_list = [rand_desde_cero(list_to_integer(maps:get("cpu", Head, "0"))),
                        rand_desde_cero(list_to_integer(maps:get("mem", Head, "0"))),
                        rand_desde_cero(list_to_integer(maps:get("gpu", Head, "0")))],
            case Rand_list of
                [0, 0, 0] -> "";

                [First, Second, Third] -> 
                    "@" ++ maps:get("host", Head) ++ "cpu" ++ 
                    integer_to_list(First) ++ "mem" ++ 
                    integer_to_list(Second) ++ "gpu" ++ 
                    integer_to_list(Third)
            end;
        
        [Head | Tail] ->
            Rand_list = [rand_desde_cero(list_to_integer(maps:get("cpu", Head, "0"))),
                        rand_desde_cero(list_to_integer(maps:get("mem", Head, "0"))), % se trabaja con MB (multiplicar y dividir por 1024)
                        rand_desde_cero(list_to_integer(maps:get("gpu", Head, "0")))],
            case Rand_list of
                [0, 0, 0] -> "" ++ armar_comando(Tail);

                [First, Second, Third] -> 
                    "@" ++ maps:get("host", Head) ++ "cpu" ++ 
                    integer_to_list(First) ++ "mem" ++ 
                    integer_to_list(Second) ++ "gpu" ++ 
                    integer_to_list(Third) ++ " " ++ armar_comando(Tail)
            end
    
    end. % JOB_REQUEST job_id @host:res:amount:res:...

% Detiene la ejecución del programa por Ms milisegundos
sleep(Ms) ->

    receive
        after
            Ms -> ok
    end.

% Vigila el estado del job
job_handler(Job_id) ->
    todo.

% loop principal del server (proceso master) luego de organizar la información de nodos
% Después de N procesos, hay una pequeña pausa
serverloop(Maps_list, Socket, N) ->
    Comando = armar_comando(Maps_list),
    case Comando of
        "" -> nada;
    
        _ -> 
            Job_id = erlang:unique_integer(),
            ("JOB_REQUEST " ++ integer_to_list(Job_id) ++ " " ++ Comando) ! Socket,
            spawn(?MODULE, job_handler, [Job_id])
    end,

    case N of
        0 -> 
            sleep(5000),
            serverloop(Maps_list, Socket, ?RAFAGA);
        
        _ -> serverloop(Maps_list, Socket, N - 1)

    end.


% Función para conectar el agente C con el programa en Erlang
% y recibir datos sobre nodos
start() ->
    rand:seed(exsp),
    % 1. Conectamos con el agente
    case gen_tcp:connect(?HOST, ?PORT, [binary, {packet, 0}, {active, false}]) of
        {ok, Socket} ->
            io:format("Conectado al agente C en el puerto ~p~n", [?PORT]),
            register(master, self()),
            
            %% 2. Solicitamos lista de nodos
            gen_tcp:send(Socket, ?GET_NODES),
            io:format("Mensaje enviado: ~s~n", [?GET_NODES]),
            
            %% 3. Esperar la respuesta de C (hasta 5 segundos)
                % y recibir los datos
            case gen_tcp:recv(Socket, 0, 5000) of
                "NODES " ++ Data ->
                    io:format("Recibido: ~s~n", [Data]),
                    Data_listed = string:split(Data, <<";">>, all), % Armamos una lista de los nodos
                    map_gen(Data_listed), 
                    Maps_list = get_list_maps(), % 4. Armamos una lista de mapas con host y n° de cpu, memoria y gpu
                    serverloop(Maps_list, Socket, ?RAFAGA),
                    gen_tcp:close(Socket);

                _ ->
                    io:format("Error: No se pudo recibir la información sobre los nodos~n"),
                    %io:format("Error: ~p~n", [Reason]),
                    gen_tcp:close(Socket)
            end;
            
        {error, Reason} ->
            io:format("Conexión fallida: ~p~n", [Reason])
    end.

%start() ->

    %Conectarse al agente C y recibir nodos vivos en formato texto
    %connection_set(),

    %Consultar por la lista de nodos vivos en formato texto (GET_NODES)
    %NODES 192.168.1.10:8100:cpu:4:mem:8192:gpu:1;192.168.1.11:8101:cpu:2:mem:4096
    
    %Lanzar simultáneamente jobs

    %implementar estrategia antideadlock

    %registrar concesiones, denegaciones y casos de deadlock en un log
    %todo.