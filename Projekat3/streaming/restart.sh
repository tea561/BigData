docker container stop prediction-streaming
docker container rm prediction-streaming
docker image rm prediction-streaming
docker compose -f docker-compose-streaming.yml up -d