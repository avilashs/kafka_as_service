#!/usr/bin/env bash
# Azure fault injections used by the M6 chaos scenario (deploy/kind/chaos.sh with CHAOS_PROVIDER=azure).
#
# STATUS: NOT RUN. Written from the documented behaviour of the Azure CLI; nothing here has touched Azure.
#
#   chaos.sh node-terminate <k8s-node-name>      delete the VM scale set instance behind a node (the node, its pods and
#                                                its disk attachments go; the managed disks stay and can be re-attached)
#   chaos.sh zone-isolate <zone> [outputs.json]  deny all traffic into and out of the subnet of a zone with
#                                                network security group rules of priority 100 (what the zone-outage
#                                                test means by "NSG-based zone isolation": the machines keep running,
#                                                the network is cut)
#   chaos.sh zone-restore <zone> [outputs.json]  remove those rules
#
# <zone> is the zone number (1, 2, 3) or the node label value (<location>-<zone>). Credentials come from `az login`;
# the resource group and NSG names come from the tofu outputs.
set -euo pipefail
cmd=${1:?usage: chaos.sh node-terminate|zone-isolate|zone-restore ...}; shift

out() { jq -r ".$1.value" "${OUTPUTS:?path to the tofu outputs json}"; }
zone_number() { echo "${1##*-}"; }
nsg_name() { jq -r --arg z "$(zone_number "$1")" '.zone_nsgs.value[$z] // empty' "${2:-${OUTPUTS:?path to the tofu outputs json}}"; }
rg() { jq -r '.resource_group.value' "${1:-${OUTPUTS:?path to the tofu outputs json}}"; }

case "$cmd" in
  node-terminate)
    node=${1:?node name}
    pid=$(kubectl get node "$node" -o jsonpath='{.spec.providerID}')   # azure:///subscriptions/<s>/resourceGroups/<rg>/providers/Microsoft.Compute/virtualMachineScaleSets/<vmss>/virtualMachines/<id>
    nrg=$(sed -n 's#.*/resourceGroups/\([^/]*\)/.*#\1#p' <<<"$pid"); vmss=$(sed -n 's#.*/virtualMachineScaleSets/\([^/]*\)/.*#\1#p' <<<"$pid"); inst=${pid##*/}
    [[ -n "$nrg" && -n "$vmss" && -n "$inst" ]] || { echo "cannot parse providerID '$pid' of node $node" >&2; exit 1; }
    echo "deleting instance $inst of scale set $vmss ($node)"
    az vmss delete-instances -g "$nrg" -n "$vmss" --instance-ids "$inst" --no-wait
    ;;
  zone-isolate)
    z=${1:?zone}; nsg=$(nsg_name "$z" "${2:-}"); [[ -n "$nsg" ]] || { echo "no NSG for zone $z" >&2; exit 1; }
    echo "isolating zone $z (NSG $nsg, deny rules of priority 100 in and out)"
    for d in Inbound Outbound; do
      az network nsg rule create -g "$(rg "${2:-}")" --nsg-name "$nsg" -n "streamcell-chaos-$d" --priority 100 --direction "$d" --access Deny \
        --protocol '*' --source-address-prefixes '*' --source-port-ranges '*' --destination-address-prefixes '*' --destination-port-ranges '*' >/dev/null
    done
    ;;
  zone-restore)
    z=${1:?zone}; nsg=$(nsg_name "$z" "${2:-}"); [[ -n "$nsg" ]] || { echo "no NSG for zone $z" >&2; exit 1; }
    echo "restoring zone $z (NSG $nsg)"
    for d in Inbound Outbound; do az network nsg rule delete -g "$(rg "${2:-}")" --nsg-name "$nsg" -n "streamcell-chaos-$d" || true; done
    ;;
  *) echo "unknown command $cmd" >&2; exit 2 ;;
esac
