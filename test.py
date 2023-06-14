import os
from ediel import TwoWireMMRParser

test_files = []
for root, dirs, files in os.walk("data/2wire/"):
    for file in files:
        if file.endswith(".csv"):
             test_files.append(os.path.join(root, file))

for f in test_files:
    parser = TwoWireMMRParser(file=f)
    print(parser.file)
    print(parser.dict['Subject'])
    df = parser.get_dataframe()
    assert not df.empty
    m = parser.get_metadata_frame(allow_duplicate_names=False)
    assert not m.empty
    ts = parser.get_timeseries_frame(allow_duplicate_names=False)
    assert not ts.empty