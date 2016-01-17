Swissim project is deployed for preview in the [DAPLAB](http://daplab.ch) cluster, hosted with ❤ by the DAPLAB team.

Below you'll find examples requests and response interacting with the [API](api.md)

## Submit the request

Submitting the request is fairly easy once you get your molecule fingerprint.

```bash
curl -vvvv -X POST --data '{ "fingerprint": "0001801a018002082100110810b00610001c106000004020030098020000110806100100400048e00100a00c608188000200980702600201816600c1020c000c080000080400800802e0200800022801c010008018008138218000418970041800086000c008014020048000000400880400420420c6000860260000614306a6", "limit": 100, "threshold": 0.8 }' https://swisssim.daplab.ch/api/v1/submit
...
< Location: /api/v1/result/c94480a6-9e03-4ca1-b458-b04ea7603551
...
```

Please save the `Location` URI to be able to retrieve the results.


## Poll the result

As the processing might take some time before it compares your molecule with the multiple hundreds of millions of molecules,
the call to retrieve the result might return a HTTP status code `202`, meaning that the sever is still processing
your query. You have to retry the same API call until you get a `200` status code:

```bash
curl -vvvv https://swisssim.daplab.ch/api/v1/result/c94480a6-9e03-4ca1-b458-b04ea7603551
...
< HTTP/1.1 202 Accepted
...
```

The HTTP code `202` indicates that the query is running and you should retry in few seconds.


## Retrieve the content

The query to retrieve the content is the same, and a HTTP code `200` is returned indicating that the result of the
processing will follow:

```bash
curl -vvvv https://swisssim.daplab.ch/api/v1/result/c94480a6-9e03-4ca1-b458-b04ea7603551
...
< HTTP/1.1 200 Ok
...
[{ "fingerprint": "00018012018002082140010010b00600001c006000004020020098020000100006100100400048e00100200c600188000200b80600600201806600c0020c400c080000180000800800e0000000022801c000008018008138218000418970000800086000c00801402000800000040008040040042046000860040000404306a0", "smile": "Cc1nc2cc(Cl)c(cc2c(=O)n1c1ccccc1O)S(=O)(=O)N", "similarity": 0.7941176470588235, "details": "; Nc1cc(Cl)c(cc1C(O)=O)S(N)(=O)=O ; 1287030|FLUKA ; SigmaAldrich ; CC(=O)Nc1ccccc1O ; A7000|ALDRICH ; SigmaAldrich ; 0008-0000014351 ; Niementowski_quinazoline" }
 { "fingerprint": "0000801a018002082100100a10b00610800c106000004020030099020000110006104100400008e00100a00c6081880002049c0702600201816e00c1020e402c080820080c00820000e0000800026801c000008018008938218400418970041800082080c0080140300880000004008c0400460420c6000860240000600306e0", "smile": "CCCC(C)(C)c1nc2ccc(cc2c(=O)n1c1ccc(Cl)cc1)S(=O)(=O)Nc1ccccc1OC", "similarity": 0.7936507936507936, "details": "; COc1ccccc1NS(=O)(=O)c1ccc(N)c(c1)C(O)=O ; CDS010681|ALDRICH ; SigmaAldrich ; CCCC(C)(C)C(=O)Nc1ccc(Cl)cc1 ; 36172|FLUKA ; SigmaAldrich ; 0008-0000098610 ; Niementowski_quinazoline" }
 ...
]
```

That's it. Easy isn't it? Just keep in mind that:

- There're no SLA on Swisssim, your query might be queued for a while
- The output of the processing is kept 1 hour before being discarded and returning a `404`
- If you find cool writing such piece of software, join us every Thursday evening for our weekly Hacky Thursdays!
