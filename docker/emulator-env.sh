# Points the AWS tools of the current shell — the AWS CLI, the AWS SDKs, the Serverless
# Framework — at the emulator docker-compose.yml runs. SOURCE it, after the emulator is up:
#
#   . docker/emulator-env.sh
#   serverless deploy --stage local
#
# The emulator generates its keys and its certificate each time it starts, so source this
# again after restarting it. The keys are the emulator's own and mean nothing to AWS:
# a tool that ignored AWS_ENDPOINT_URL and went to AWS would be refused there.

_emulator_port="${EMULATOR_PORT:-9297}"
# The directory of this file: bash names a sourced file in BASH_SOURCE, zsh in $0.
_emulator_home="$(cd "$(dirname "${BASH_SOURCE:-$0}")" && pwd)/emulator-home/${_emulator_port}"

if [ ! -f "${_emulator_home}/emulator_credentials" ] || [ ! -f "${_emulator_home}/ca.pem" ]; then
  echo "emulator-env: no running emulator has written ${_emulator_home}" >&2
  echo "emulator-env: start it with: docker compose -f docker/docker-compose.yml up -d" >&2
  unset _emulator_port _emulator_home
  return 1 2>/dev/null || exit 1
fi

# Every service's endpoint, for the AWS CLI v2 and the SDKs that read this variable.
export AWS_ENDPOINT_URL="https://127.0.0.1:${_emulator_port}"
export AWS_REGION=us-east-1
export AWS_DEFAULT_REGION=us-east-1

# The emulator's self-signed certificate: AWS_CA_BUNDLE for the CLI and the SDKs that read
# it, NODE_EXTRA_CA_CERTS for anything that runs on Node.js.
export AWS_CA_BUNDLE="${_emulator_home}/ca.pem"
export NODE_EXTRA_CA_CERTS="${_emulator_home}/ca.pem"

# The keys this run of the emulator accepts, in place of whatever the shell had.
AWS_ACCESS_KEY_ID="$(sed -n 's/^aws_access_key_id *= *//p' "${_emulator_home}/emulator_credentials")"
AWS_SECRET_ACCESS_KEY="$(sed -n 's/^aws_secret_access_key *= *//p' "${_emulator_home}/emulator_credentials")"
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
unset AWS_SESSION_TOKEN AWS_PROFILE

echo "emulator-env: AWS tools in this shell now use ${AWS_ENDPOINT_URL}"
unset _emulator_port _emulator_home
