# from vertex_protocol.client import create_vertex_client
# private_key = "fb4b224fd7b27229f7fcfe5e2448bace2368aaf64593bb1e02d452b688c2c450"
# client = create_vertex_client("mainnet", private_key)
"""
滚仓原理：
滚仓约束：
1、重仓后不允许亏损出现
2、首仓允许止损出现
3、在首仓盈利后，如果新止损点到来，则会调整新止损，然后根据新止损点调整，提高仓位

中阳出现后，可能意味着新的止损点到来，中阳一般是次级别从下轨到上轨的实现
"""