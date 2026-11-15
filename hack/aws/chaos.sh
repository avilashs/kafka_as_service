#!/usr/bin/env bash
# AWS fault injections used by the M6 chaos scenario (deploy/kind/chaos.sh with CHAOS_PROVIDER=aws).
#
# STATUS: NOT RUN. Written from the documented behaviour of the AWS CLI; nothing here has touched AWS.
#
#   chaos.sh node-terminate <k8s-node-name>   terminate the EC2 instance behind a node (the node, its pods and its
#                                             volume attachments go; the EBS volumes stay and can be re-attached)
#   chaos.sh zone-isolate <az> [outputs.json] deny all traffic into and out of the private subnet of a zone with
#                                             network ACL rule 50 (what the zone-outage test means by "NACL-based
#                                             zone isolation": the machines keep running, the network is cut)
#   chaos.sh zone-restore <az> [outputs.json] remove those rules
#
# Region, credentials and the Terraform outputs (for the NACL ids) are taken from the environment / the file.
set -euo pipefail
cmd=${1:?usage: chaos.sh node-terminate|zone-isolate|zone-restore ...}; shift

nacl_id() { # az outputs.json
  jq -r --arg az "$1" '.private_nacl_ids.value[$az] // empty' "${2:-${OUTPUTS:?path to the tofu outputs json}}"
}

case "$cmd" in
  node-terminate)
    node=${1:?node name}
    id=$(kubectl get node "$node" -o jsonpath='{.spec.providerID}' | sed 's#.*/##')
    [[ "$id" == i-* ]] || { echo "no EC2 instance id for node $node (providerID '$id')" >&2; exit 1; }
    echo "terminating $id ($node)"
    aws ec2 terminate-instances --instance-ids "$id" >/dev/null
    ;;
  zone-isolate)
    az=${1:?availability zone}; nacl=$(nacl_id "$az" "${2:-}"); [[ -n "$nacl" ]] || { echo "no network ACL for $az" >&2; exit 1; }
    echo "isolating $az (network ACL $nacl, rule 50 deny in and out)"
    aws ec2 create-network-acl-entry --network-acl-id "$nacl" --rule-number 50 --protocol -1 --rule-action deny --ingress --cidr-block 0.0.0.0/0
    aws ec2 create-network-acl-entry --network-acl-id "$nacl" --rule-number 50 --protocol -1 --rule-action deny --egress  --cidr-block 0.0.0.0/0
    ;;
  zone-restore)
    az=${1:?availability zone}; nacl=$(nacl_id "$az" "${2:-}"); [[ -n "$nacl" ]] || { echo "no network ACL for $az" >&2; exit 1; }
    echo "restoring $az (network ACL $nacl)"
    aws ec2 delete-network-acl-entry --network-acl-id "$nacl" --rule-number 50 --ingress || true
    aws ec2 delete-network-acl-entry --network-acl-id "$nacl" --rule-number 50 --egress || true
    ;;
  *) echo "unknown command $cmd" >&2; exit 2 ;;
esac
