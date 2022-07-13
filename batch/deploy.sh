docker container prune -f && docker image prune -af && docker compose build
docker push undpgeohub.azurecr.io/sids-data-batch
az container create --resource-group undpdpbppssdganalyticsgeo --file deploy.yml
