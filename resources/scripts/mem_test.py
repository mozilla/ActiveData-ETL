import requests
from pyLibrary.debugs.logs import Log

while True:
    url="http://mozilla-releng-blobs.s3.amazonaws.com/blobs/b2g-inbound/sha512/3f836a35a74a3a7655807566e54c9991d3f765b98ea6d8f7a953bf48bc51eb5e8d2a768635fcd6e89d3adc54af4545a61d8b7520939d75d122ef0f6c428c056b"
    v = requests.get(url).content
    Log.note(v)

