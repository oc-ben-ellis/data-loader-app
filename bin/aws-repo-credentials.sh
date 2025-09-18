#!/bin/bash -e

# N.B. This script must be sourced, rather than executed, to get the environment variables into the correct environment
# Note: You must set up your AWS config for the development account, see https://opencorporates.atlassian.net/wiki/x/AYDcCQ
AWS_REGION="eu-west-2"
AWS_PROFILE="${AWS_PROFILE:-development}"
FORCE_RENEWAL="${FORCE_RENEWAL:-1}"

NO_AWS_PROFILE="${NO_AWS_PROFILE:-FALSE}"

function aws_profile() {
  if [ "${NO_AWS_PROFILE}" = "FALSE" ]; then
    echo "--profile ${AWS_PROFILE}"
  fi
}

# CodeArtifact resides in the management account but access is allowed via the AWS Organization
function get_token_details() {
  # shellcheck disable=SC2046
  aws codeartifact get-authorization-token \
    --domain open-corporates \
    --domain-owner 089449186373 \
    --region "$AWS_REGION" \
    $(aws_profile) \
    --output text \
    --query "[authorizationToken,expiration]"
}

CURRENT_EPOCH=$(date +%s)
if [ "${FORCE_RENEWAL}" != "0" ] || [ -z "${POETRY_HTTP_BASIC_OCPY_EXPIRES}" ] || [ "${CURRENT_EPOCH}" -ge "$(date --date="${POETRY_HTTP_BASIC_OCPY_EXPIRES}" +%s)" ]; then
  # shellcheck disable=SC2046
  if aws sts get-caller-identity $(aws_profile) &>/dev/null; then
    echo "AWS SSO already logged in"
  else
    echo "Need to login to AWS SSO"
    # avoid weird behaviour with multiple browser profiles
    aws sso login $(aws_profile) --no-browser
  fi

  TOKEN_DETAILS="$(get_token_details)"
  export POETRY_HTTP_BASIC_OCPY_EXPIRES="${TOKEN_DETAILS##*$'\t'}"
  export POETRY_HTTP_BASIC_OCPY_USERNAME="aws"
  export POETRY_HTTP_BASIC_OCPY_PASSWORD="${TOKEN_DETAILS%%$'\t'*}"

  echo "exported POETRY_HTTP_BASIC_OCPY_EXPIRES=${POETRY_HTTP_BASIC_OCPY_EXPIRES}"
  echo "exported POETRY_HTTP_BASIC_OCPY_USERNAME=${POETRY_HTTP_BASIC_OCPY_USERNAME}"
  echo "exported POETRY_HTTP_BASIC_OCPY_PASSWORD=${POETRY_HTTP_BASIC_OCPY_PASSWORD:0:5}"
else
  echo "Current token has not expired."
  echo "no change to POETRY_HTTP_BASIC_OCPY_EXPIRES=${POETRY_HTTP_BASIC_OCPY_EXPIRES}"
  echo "no change to POETRY_HTTP_BASIC_OCPY_USERNAME=${POETRY_HTTP_BASIC_OCPY_USERNAME}"
  echo "no change to POETRY_HTTP_BASIC_OCPY_PASSWORD=${POETRY_HTTP_BASIC_OCPY_PASSWORD:0:5}"
fi
