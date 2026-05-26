#!/usr/bin/env bash
set -euo pipefail

# Parâmetros (sobrescrevíveis por variável de ambiente). O Account ID NÃO é
# embutido aqui — é descoberto em runtime a partir do perfil AWS logado.
PROFILE="${AWS_PROFILE:-operacional}"
REGION="${AWS_REGION:-us-east-1}"
REPO="spdo-apps"
TAG="app-identifica-coleta-v6"

aws sso login --profile "$PROFILE"

ACCOUNT_ID="$(aws sts get-caller-identity --profile "$PROFILE" --query Account --output text)"
REGISTRY="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"

docker build -t "${REPO}:${TAG}" .
docker tag "${REPO}:${TAG}" "${REGISTRY}/${REPO}:${TAG}"
aws ecr get-login-password --region "$REGION" --profile "$PROFILE" \
    | docker login --username AWS --password-stdin "$REGISTRY"
docker push "${REGISTRY}/${REPO}:${TAG}"
