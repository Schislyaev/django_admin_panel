#!/bin/sh

echo "Waiting for postgres..."
while ! nc -z $DB_HOST $DB_PORT; do
  sleep 0.1
done
echo "PostgreSQL started"

echo "Collecting static..."
python manage.py collectstatic --no-input
echo " ...done"

uwsgi --strict --ini uwsgi.ini

exec "$@"