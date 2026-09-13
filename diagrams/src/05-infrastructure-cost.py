"""UrbanRide — Deployment and Cost Topology (ADD Section 5, Member 5).

Renders diagrams/export/05-infrastructure-cost.png using official AWS icons.

    python -m venv venv && venv/bin/pip install diagrams   # needs graphviz installed
    venv/bin/python diagrams/src/05-infrastructure-cost.py

Deliberately shows infrastructure tiers, purchase models and per-tier cost.
Service-to-service call flow belongs to Member 3's C4 container diagram.
"""

from diagrams import Diagram, Cluster, Edge
from diagrams.aws.analytics import ManagedStreamingForKafka
from diagrams.aws.compute import EC2Instances, EC2SpotInstance, Lambda
from diagrams.aws.database import Aurora, Dynamodb, ElastiCache
from diagrams.aws.general import General, Users
from diagrams.aws.network import ElbApplicationLoadBalancer, ElbNetworkLoadBalancer, Route53
from diagrams.aws.storage import S3, S3Glacier

GRAPH = {
    "fontsize": "26",
    "fontname": "Helvetica",
    "bgcolor": "white",
    "pad": "0.9",
    "nodesep": "1.30",
    "ranksep": "1.50",
    "compound": "true",
    "labelloc": "t",
    "splines": "ortho",
}
# The diagrams library leaves `imagepos` unset, so graphviz centres the icon
# vertically and draws the bottom-anchored caption on top of it. Pinning the
# icon to top-centre reserves the lower half of the node for the caption.
NODE = {"fontsize": "15", "fontname": "Helvetica", "imagepos": "tc"}
EDGE = {"fontsize": "13", "fontname": "Helvetica", "color": "#4A4A4A"}

ASYNC = Edge(style="dashed", color="#EF6C00")

with Diagram(
    "UrbanRide — Deployment and Cost Topology  ·  3 region cells  ·  ~$42,700/month  ·  1.37 LKR per ride",
    filename="diagrams/export/05-infrastructure-cost",
    outformat="png",
    show=False,
    direction="TB",
    graph_attr=GRAPH,
    node_attr=NODE,
    edge_attr=EDGE,
):

    users = Users("Riders and drivers")
    dns = Route53("Route 53\nlatency routing")

    with Cluster("Region cell — eu-west-1 · 3 Availability Zones · ≈$11,200/month",
                 graph_attr={"margin": "26", "bgcolor": "#FFFDF5", "fontsize": "19", "pencolor": "#B0A268"}):

        with Cluster("Edge", graph_attr={"margin": "26", "bgcolor": "#F5F5F5", "fontsize": "15"}):
            alb = ElbApplicationLoadBalancer("ALB\nHTTPS / REST")
            nlb = ElbNetworkLoadBalancer("NLB\nWSS — GPS pings")

        with Cluster("Amazon EKS  ·  auto-scales 6 → 60 nodes  ·  $2,822/mo",
                     graph_attr={"margin": "26", "bgcolor": "#FBFBFB", "fontsize": "17"}):

            with Cluster("On-Demand baseline · Savings Plan\n"
                         "API Gateway · Matching Engine · Identity & Profile",
                         graph_attr={"margin": "26", "bgcolor": "#E3F2FD", "fontsize": "15", "pencolor": "#1565C0"}):
                ondemand = EC2Instances("6 nodes · 8 vCPU each\n$0.303/h blended")

            with Cluster("Spot burst pool · 52% cheaper\n"
                         "Location Ingestion · Trip Management · Surge Pricing · Billing",
                         graph_attr={"margin": "26", "bgcolor": "#FFF3E0", "fontsize": "15", "pencolor": "#EF6C00"}):
                spot = EC2SpotInstance("14 nodes · 8 vCPU each\n$0.146/h blended")

        with Cluster("Managed data tier — multi-AZ  ·  $3,671/mo incl. MSK Connect",
                     graph_attr={"margin": "26", "bgcolor": "#E8F5E9", "fontsize": "17", "pencolor": "#2E7D32"}):

            with Cluster("Stream and geo cache",
                         graph_attr={"margin": "26", "bgcolor": "#DCEFDD", "fontsize": "14"}):
                msk = ManagedStreamingForKafka("Amazon MSK + Connect\n6 brokers + Debezium CDC\n$1,265/mo")
                cache = ElastiCache("ElastiCache Valkey\n3 shards × 2\n$959/mo")

            with Cluster("Systems of record",
                         graph_attr={"margin": "26", "bgcolor": "#DCEFDD", "fontsize": "14"}):
                aurora = Aurora("Aurora PostgreSQL\nwriter + 2 readers\n$1,247/mo")
                ddb = Dynamodb("DynamoDB\ndriver / trip state\n$200/mo")

        with Cluster("Low-frequency and object storage",
                     graph_attr={"margin": "26", "bgcolor": "#F3F7F3", "fontsize": "15"}):
            lam = Lambda("Lambda\nreceipts, reconciliation\n$60/mo")
            s3 = S3("S3 — Parquet\n81 GB/day\n$300/mo")

    peers = General("2 further cells\nap-south-1 · us-east-1\nidentical, ~$11,200/mo each")

    with Cluster("Global layer — asynchronous only, never on the matching path",
                 graph_attr={"margin": "26", "bgcolor": "#EDE7F6", "fontsize": "17", "pencolor": "#5E35B1"}):
        glacier = S3Glacier("Glacier Deep Archive\n7-year retention\n$0.00099/GB-mo")
        ddbg = Dynamodb("DynamoDB global tables\nIdentity & Profile")

    # Request path
    users >> dns
    dns >> Edge(label="home cell") >> alb
    dns >> nlb
    dns >> peers
    alb >> Edge(label="REST") >> ondemand
    nlb >> Edge(label="37,500 pings/s") >> spot

    # Compute to data
    ondemand >> Edge(label="geo lookup") >> cache
    spot >> ASYNC >> msk
    spot >> aurora

    # Streaming and tiering
    msk >> ASYNC >> s3
    spot >> ASYNC >> lam
    s3 >> Edge(style="dashed", color="#5E35B1", label="lifecycle tiering") >> glacier
    ddb >> Edge(style="dashed", color="#5E35B1") >> ddbg
