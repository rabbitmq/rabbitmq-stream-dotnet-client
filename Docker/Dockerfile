FROM  pivotalrabbitmq/rabbitmq-stream


ENV TZ=Europe/Minsk
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone


RUN wget https://packages.microsoft.com/config/ubuntu/21.04/packages-microsoft-prod.deb -O packages-microsoft-prod.deb
RUN dpkg -i packages-microsoft-prod.deb
RUN rm packages-microsoft-prod.deb

RUN apt-get update && \
   apt-get install -y apt-transport-https && \
   apt-get update && \
   apt-get install -y git && \
   apt-get install -y dotnet-sdk-6.0

RUN apt-get install make -y
