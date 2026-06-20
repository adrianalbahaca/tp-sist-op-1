-module(c_agent_client).
-export([start/0, map_gen/1, node_map/1, armar_lista/1]).
-export([get_list_maps/0, serverloop/0]).

-define(PORT, 8000).
-define(HOST, "localhost").
-define(GET_NODES, <<"GET_NODES">>).

% 1.22.312:1212:cpu:1:mem:2212:gpu:1
armar_lista(Node_listed) -> 
    case Node_listed of
        [] -> [];
        ["host", Second, Third | Tail] -> [{"host", Second}] ++ armar_lista(Tail);
        [First, Second | Tail] -> [{First, Second}] ++ armar_lista(Tail)
    end.

% Función de manejo de job
% 
node_map(Node_listed) ->
    Node_paired = armar_lista(Node_listed),
    Map = maps:from_list(Node_paired),
    master ! Map,
    done.

% Generador de mapas para saber más facilmente
% los recursos disponibles
% Se generan a partir de una lista de nodos
map_gen(Nodes) ->
    case Nodes of
        
        [] -> generation_done;
        
        [Head | Tail] -> 
            Head_listed = string:split(Head, <<":">>, all),
            spawn(?MODULE, node_map, [["host"] ++ Head_listed]),
            map_gen(Tail)

    end.

get_list_maps() ->
    
    io:fwrite("Acomodando datos, espere...~n"), %%%
    receive
        Mapa -> [Mapa] ++ get_list_maps()

        after
        5000 -> maps_are_here
    end.

serverloop() ->
    todo.


% Función para conectar el agente C con el programa en Erlang
start() ->

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
                    io:format("Recibido: ~s~n", [Data]), % considerar agregar caso error en Data antes de formatear
                    Data_listed = string:split(Data, <<";">>, all), % Armamos una lista de los nodos
                    map_gen(Data_listed), 
                    Maps_list = get_list_maps(), % 4. Armamos una lista de mapas con host y n° de cpu, gpu y memoria
                    serverloop(),
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