{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowPassingInstanceRole",
      "Effect": "Allow",
      "Resource": "${node_role_arn}",
      "Action": "iam:PassRole",
      "Condition": {
        "StringEquals": {
          "iam:PassedToService": [
            "ec2.amazonaws.com",
            "ec2.amazonaws.com.cn"
          ]
        }
      }
    },
    {
      "Sid": "AllowScopedInstanceProfileCreationActions",
      "Effect": "Allow",
      "Resource": "arn:${partition}:iam::${account_id}:instance-profile/*",
      "Action": [
        "iam:CreateInstanceProfile"
      ],
      "Condition": {
        "StringEquals": {
          "aws:RequestTag/kubernetes.io/cluster/${cluster_name}": "owned",
          "aws:RequestTag/eks:eks-cluster-name": "${cluster_name}",
          "aws:RequestTag/topology.kubernetes.io/region": "${region}"
        },
        "StringLike": {
          "aws:RequestTag/karpenter.k8s.aws/ec2nodeclass": "*"
        }
      }
    },
    {
      "Sid": "AllowScopedInstanceProfileTagActions",
      "Effect": "Allow",
      "Resource": "arn:${partition}:iam::${account_id}:instance-profile/*",
      "Action": [
        "iam:TagInstanceProfile"
      ],
      "Condition": {
        "StringEquals": {
          "aws:ResourceTag/kubernetes.io/cluster/${cluster_name}": "owned",
          "aws:ResourceTag/topology.kubernetes.io/region": "${region}",
          "aws:RequestTag/kubernetes.io/cluster/${cluster_name}": "owned",
          "aws:RequestTag/eks:eks-cluster-name": "${cluster_name}",
          "aws:RequestTag/topology.kubernetes.io/region": "${region}"
        },
        "StringLike": {
          "aws:ResourceTag/karpenter.k8s.aws/ec2nodeclass": "*",
          "aws:RequestTag/karpenter.k8s.aws/ec2nodeclass": "*"
        }
      }
    },
    {
      "Sid": "AllowScopedInstanceProfileActions",
      "Effect": "Allow",
      "Resource": "arn:${partition}:iam::${account_id}:instance-profile/*",
      "Action": [
        "iam:AddRoleToInstanceProfile",
        "iam:RemoveRoleFromInstanceProfile",
        "iam:DeleteInstanceProfile"
      ],
      "Condition": {
        "StringEquals": {
          "aws:ResourceTag/kubernetes.io/cluster/${cluster_name}": "owned",
          "aws:ResourceTag/topology.kubernetes.io/region": "${region}"
        },
        "StringLike": {
          "aws:ResourceTag/karpenter.k8s.aws/ec2nodeclass": "*"
        }
      }
    }
  ]
}
