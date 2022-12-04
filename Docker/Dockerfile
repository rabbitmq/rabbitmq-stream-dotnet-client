FROM  pivotalrabbitmq/rabbitmq-stream


ENV TZ=Europe/Minsk
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone


RUN wget https://packages.microsoft.com/config/ubuntu/21.04/packages-microsoft-prod.deb -O packages-microsoft-prod.deb
RUN dpkg -i packages-microsoft-prod.deb
RUN rm packages-microsoft-prod.deb


RUN printf 'Package: *net*\nPin: origin packages.microsoft.com\nPin-Priority: 1001' >> /etc/apt/preferences

RUN apt-get update 
RUN apt install dotnet6 -y
RUN apt install git -y


RUN apt-get install make -y
