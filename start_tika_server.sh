#!/bin/bash

TIKA_PORT=9998
TIKA_HOST=localhost
CURRENT_USER=$(whoami) 
TIKA_JAR_URL="http://search.maven.org/remotecontent?filepath=org/apache/tika/tika-server/1.19/tika-server-1.19.jar"
TIKA_WORKSPACE=$HOME/tika
TIKA_FILE_NAME="tika_server.jar"

echo -e "Usuario corrent $CURRENT_USER"

if [ ! -f $TIKA_WORKSPACE/$TIKA_FILE_NAME ]; then
	echo -e "Fazendo download do tika-server.jar"

	if [ ! -d "$TIKA_WORKSPACE" ]; then
		echo -e "Criando diretorio de trabalho do tika"
		mkdir $TIKA_WORKSPACE
	fi

	wget -c $TIKA_JAR_URL -O $TIKA_WORKSPACE/$TIKA_FILE_NAME 
fi

echo -e "## Setando variaveis de ambiente"

export TIKA_SERVER_ENDPOINT="http://$TIKA_HOST:$TIKA_PORT"
echo -e "TIKA_SERVER_ENDPOINT para $TIKA_SERVER_ENDPOINT"

export TIKA_CLIENT_ONLY=True
echo -e "TIKA_CLIENT_ONLY para $TIKA_CLIENT_ONLY"

echo -e "## Iniciando o servidor tika em $TIKA_WORKSPACE"
cd $TIKA_WORKSPACE

java -jar tika_server.jar -h $TIKA_HOST 