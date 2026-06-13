-module(c_agent_client).
-export([start/0, job_generator/1, handle_job/1]).

-define(PORT, 8080).
-define(HOST, "localhost").
-define(GET_NODES, <<"GET_NODES">>).

% Función de manejo de job
handle_job(Node) ->

    todo.

% Generador de los procesos que manejarán los jobs (uno cada uno).
% Se generan a partir de una lista de nodos
job_generator(Nodes) ->
    case Nodes of
        [Head | Tail] -> 
            spawn(?MODULE, handle_job, [Head]),
            job_generator(Tail);

        _ -> generation_done
    end.

% Función para conectar el agente C con el programa en Erlang
start() ->

    % 1. Conectamos con el agente
    case gen_tcp:connect(?HOST, ?PORT, [binary, {packet, 0}, {active, false}]) of
        {ok, Socket} ->
            io:format("Conectado al agente C en el puerto ~p~n", [?PORT]),
            
            %% 2. Solicitamos lista de nodos
            gen_tcp:send(Socket, ?GET_NODES),
            io:format("Mensaje enviado: ~s~n", [?GET_NODES]),
            
            %% 3. Esperar la respuesta de C (hasta 5 segundos)
                % y recibir los datos
            case gen_tcp:recv(Socket, 0, 5000) of
                {ok, Data} ->
                    io:format("Recibido: ~s~n", [Data]), % considerar agregar caso error en Data antes de formatear
                    Data_listed = string:split(Data, <<";">>, all), % Armamos una lista de los nodos
                    job_generator(Data_listed),
                    gen_tcp:close(Socket);

                {error, Reason} ->
                    io:format("Error: ~p~n", [Reason]),
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