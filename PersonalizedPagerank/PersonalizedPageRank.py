import os
os.environ["PYTHONWARNINGS"] = "ignore"
import warnings
warnings.filterwarnings("ignore")

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local[1]")
sc = SparkContext(appName="PersonalizedPageRank", conf=conf)
sc.setLogLevel("ERROR")

# Định nghĩa đồ thị
edges = [
    ('P1', 'P2'), ('P1', 'P3'), ('P3', 'P1'), ('P3', 'P2'), ('P3', 'P5'),
    ('P5', 'P6'), ('P5', 'P4'), ('P6', 'P4'), ('P4', 'P5'), ('P4', 'P6')
]

# Tạo RDD các cạnh và tập node
edges_rdd = sc.parallelize(edges, numSlices=1)
all_nodes = edges_rdd.flatMap(lambda x: x).distinct()

# Xây dựng danh sách kề cho từng node
links = all_nodes.map(lambda n: (n, [])) \
    .leftOuterJoin(edges_rdd.groupByKey().mapValues(list)) \
    .mapValues(lambda x: x[1] if x[1] is not None else [])

# Khởi tạo Personalized PageRank
source, d, num_iters = 'P1', 0.15, 2
ranks = all_nodes.map(lambda n: (n, 1.0 if n == source else 0.0))

# Vòng lặp PageRank
for i in range(num_iters):
    print(f"\nVòng lặp {i+1}/{num_iters}:")
    contribs = links.join(ranks).flatMap(
        # 2. Với mỗi node, chia đều PageRank cho các node kề (outbound)
        # Nếu node có danh sách kề, tạo (dst, giá trị chia đều) cho từng dst
        lambda x: [(dst, x[1][1] / len(x[1][0])) for dst in x[1][0]] if x[1][0] else []
    )

    # Cộng dồn tất cả đóng góp nhận được cho từng node
    ranks_sum = contribs.reduceByKey(lambda a, b: a + b)

    # Đảm bảo mọi node đều có giá trị PageRank (nếu không nhận được đóng góp thì là 0.0)
    ranks = all_nodes.map(lambda n: (n, 0.0)).leftOuterJoin(ranks_sum) \
        .mapValues(lambda x: x[1] if x[1] is not None else 0.0)

    # --- Bổ sung xử lý dangling nodes ---
    # Xác định các node không có outbound (dangling)
    dangling_nodes = links.filter(lambda x: len(x[1]) == 0).keys().collect()
    if dangling_nodes:
        # Tổng PageRank của các dangling nodes
        dangling_sum = ranks.filter(lambda x: x[0] in dangling_nodes).map(lambda x: x[1]).sum()
    else:
        dangling_sum = 0.0

    # Cập nhật PageRank mới cho từng node, cộng thêm phần từ dangling về source
    ranks = ranks.map(
        lambda x: (
            x[0],
            d * (1.0 if x[0] == source else 0.0)
            + (1 - d) * (x[1] + (dangling_sum if x[0] == source else 0.0))
        )
    )

    # In giá trị PageRank từng node sau mỗi vòng lặp
    for node, rank in sorted(ranks.collect()):
        print(f"  {node}: {rank:.4f}")

# In kết quả
results = ranks.collect()
max_node, max_rank = max(results, key=lambda x: x[1])
print(f"\nNode quan trọng nhất đối với node nguồn ({source}) là: {max_node} với PageRank = {max_rank:.4f}")

sc.stop()


