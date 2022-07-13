envsubst <manifest.tpl.yml >manifest.yml
docker container prune -f && docker image prune -af && docker compose build
docker push undpgeohub.azurecr.io/sids-data-batch
az container delete --resource-group undpdpbppssdganalyticsgeo --name sids-data-batch --yes
az container create --resource-group undpdpbppssdganalyticsgeo --file deploy.yml
