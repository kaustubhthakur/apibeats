docker-compose up -d
docker-compose ps
curl http://localhost:8000/workers
docker-compose down
docker-compose down -v
docker-compose logs -f
docker-compose logs -f manager
docker-compose logs -f worker1
