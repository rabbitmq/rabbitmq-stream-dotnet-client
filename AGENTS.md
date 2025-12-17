This project is a library for RabbitMQ Streams. It is written in C#.

# Project Structure

## Build

Use the `Makefile` to build the project. Use `make build` to build the project. Use `make clean` to clean the project.
When running on Darwin, use `gmake` instead of `make`.

## Tests

The tests are written in xUnit and are located in the `Tests` folder. Use the `Makefile` to run the tests.

Filter tests by running `make test TEST_FLAGS="--filter FullyQualifiedName~Tests.<ClassName>.<MethodName>"`. Where `<ClassName>` and `<MethodName>` are the name of the class and method to filter by.

When running on Darwin, use `gmake` instead of `make`.

## Documentation

The documentation is written in Asciidoc and is located in the `docs` folder. This folder contains code examples and documentation for the library.

## Source Code

The source code is located in the `RabbitMQ.Stream.Client` folder. This folder contains the library code.

The project `RabbitMQ.Stream.Client.PerfTest` is a performance test project. It is used to test the performance of the library.
This project is a CLI application.