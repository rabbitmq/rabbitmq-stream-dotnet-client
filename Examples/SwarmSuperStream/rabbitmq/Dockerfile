FROM rabbitmq:3.11.4-management-alpine
COPY rabbitmq.conf /etc/rabbitmq
COPY definitions30.json /etc/rabbitmq
RUN cat /etc/rabbitmq/rabbitmq.conf
RUN rabbitmq-plugins enable rabbitmq_stream
RUN rabbitmq-plugins enable rabbitmq_management
