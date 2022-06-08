docker run --name rabbitmq -p 5672:5672 -d rabbitmq:latest

docker run --name rabbitmq-management -p 5672:5672 -p 15672:15672 rabbitmq:management

docker run --name rabbitmq-management -p 5672:5672 -p 15672:15672 -e RABBITMQ_DEFAULT_USER=user -e RABBITMQ_DEFAULT_PASS=password rabbitmq:management