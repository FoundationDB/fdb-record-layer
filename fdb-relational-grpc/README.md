# Relational GRPC
Module of client/server GRPC proto files, as well as generated
and utility code for mix-in by client and servers.
The generated GRPC code has the client and server service stubs
in the one class and the protobuf messages used in communication
are defined here. We want to avoid the client depending (or
bundling) server-side code other than the bare protobuf api
description and messages (and vice-versa) so have the upstream
server and client modules depend on this module alone and
implement their end of the communication without having to 
have any other dependencies on each other (An important Relational
goal is a light-weight client loosely coupled to the backend so
the backend and client can be evolved fairly independently).

