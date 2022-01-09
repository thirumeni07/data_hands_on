docker run \
  -it \
  --name data-copier \
  --rm \
  --network data-copier-nw \
  -v /Users/thirumeninathanmeiyappan/Desktop/thiru/Job_Prep/data_engineering_practice/hands_on/retail_db_json:/retail_db_json \
  -e BASE_DIR=/retail_db_json \
  -e DB_HOST=e699320aa54c \
  -e DB_PORT=5432 \
  -e DB_NAME=retail_db \
  -e DB_USER=retail_user \
  -e DB_PASS=itversity \
  data-copier bash