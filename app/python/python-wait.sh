until docker-compose exec -T db mysqladmin ping -h localhost --silent; do
  echo 'Aguardando o MySQL...'
  sleep 1
done

echo 'MySQL está pronto!'