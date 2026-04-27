import urllib.request
import json

AUTH_TOKEN = "aI0OG90h6c8WUPUUgQLkSOfQTpXD6EkE"
WEBHOOK_ID = "wh_acftlzsi1290nqq2"

addresses = [
    "0x28c6c06298d514db089934071355e5743bf21d60",
    "0xb5d85cbf7cb3ee0d56b3bb207d5fc4b82f43f511",
    "0xa9ac43f5b5e38155a288d1a01d2cbc4478e14573",
    "0x4976a4a02f38326660d17bf34b431dc6e2eb2327",
    "0x9696f59e4d72e237be84ffd425dcad154bf96976",
    "0xdfd5293d8e347dfe59e90efd55b2956a1343963d",
    "0x21a31ee1afc51d94c2efccaa2092ad1028285549",
    "0xf30ba13e4b04ce5dc4d254ae5fa95477800f0eb0",
    "0x56eddb7aa87536c09ccc2793473599fd21a8b17f",
    "0xeae7380dd4cef6fbd1144f49e4d1e6964258a4f4",
    "0x0003b5aa5e30e97fcc596bb5d0f3a75255e08d4e",
    "0xf8191d98ae98d2f7abdfb63a9b0b812b93c873aa",
    "0x46340b20830761efd32832a74d7169b29feb9758",
    "0x1ab4973a48dc892cd9971ece8e01dcc7688f8f23",
    "0xa1abfa21f80ecf401bd41365adbb6fef6fefdf09",
    "0xa03400e098f4421b34a3a44a1b4e571419517687",
    "0x62425cd6bdcb6bfe51558ea465b063486b70dc9f",
    "0x5d0c7796aba12539c4caf0b9fa46ecc66c05ae9f",
    "0xf584f8728b874a6a5c7a8d4d387c9aae9172d621",
    "0x44894aeee56c2dd589c1d5c8cb04b87576967f97",
    "0x0d0707963952f2fba59dd06f2b425ace40b492fe",
    "0xa26148ae51fa8e787df319c04137602cc018b521",
    "0xf70da97812cb96acdf810712aa562db8dfa3dbef",
    "0x2cff890f0378a11913b6129b2e97417a2c302680",
    "0xf89d7b9c864f589bbf53a82105107622b35eaa40",
    "0xa29e963992597b21bcdcaa969d571984869c4ff5",
    "0xbea9f7fd27f4ee20066f18def0bc586ec221055a",
    "0x5b702b24b00a37b61ff9d71d91fed1a629edd1df",
    "0xedc7001e99a37c3d23b5f7974f837387e09f9c93",
    "0x54620b9a8a2c43aa8ed028450a7ce656a9c69feb",
    "0x974caa59e49682cda0ad2bbe82983419a2ecc400",
    "0x963737c550e70ffe4d59464542a28604edb2ef9a",
    "0x45300136662dd4e58fc0df61e6290dffd992b785",
    "0x86a067030a9668c13ff2a8c4d5415afc776d4c63",
    "0xe92e65049b3c2ca12806e9567b08895118c5a03f",
    "0x42655fc8443fb69e9ccdb94e89c1ee9317508a6d",
    "0x7c876bdaa5c038e19f633714f622f6def949b102",
    "0x9c2e658ffc8ea7fad00a4829bd4b554e8a716f73",
    "0xc0ffeebabe5d496b2dde509f9fa189c25cf29671",
    "0x9c19b0497997fe9e75862688a295168070456951",
    "0xa090e606e30bd747d4e6245a1517ebe430f0057e",
    "0x7f604d597c15b2e2f60dc645844f68b1d781b752",
    "0x12ef5e054878b225fe3d36e5aa96a3ba41eac385",
    "0x9642b23ed1e01df1092b92641051881a322f5d4e",
    "0x835033bd90b943fa0d0f8e5382d9dc568d3fbd96",
    "0x15f46551f5d736c9b8861c1d1358e29183f7a6c7",
    "0x95b262e0bd0f3cc0f886524e22ec21011c5ad3e0",
    "0xeba88149813bec1cccccfdb0dacefaaa5de94cb1",
    "0x9fc30541611132c5ac38318e8eee044d2d36996f",
]

payload = json.dumps({
    "webhook_id": WEBHOOK_ID,
    "addresses_to_add": addresses,
    "addresses_to_remove": []
}).encode()

req = urllib.request.Request(
    "https://dashboard.alchemy.com/api/update-webhook-addresses",
    data=payload,
    headers={
        "Content-Type": "application/json",
        "X-Alchemy-Token": AUTH_TOKEN
    },
    method="PATCH"
)

try:
    with urllib.request.urlopen(req, timeout=15) as r:
        print(f"Status: {r.status}")
        print(r.read().decode())
except urllib.error.HTTPError as e:
    print(f"HTTP Error: {e.code}")
    print(e.read().decode())
except Exception as e:
    print(f"Error: {e}")
