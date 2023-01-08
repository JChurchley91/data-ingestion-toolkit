from utils.gcp import GcpCredentials

gcp = GcpCredentials()
gcp = gcp.return_write_config()
print(gcp.__dict__["_properties"]["load"]["writeDisposition"])
