#!/bin/bash

curl -# -k -o IPv4HC.result "$MM_URL/feeds/IPv4HC" -o IPv4HC.result
curl -# -k -o IPv4HC%3Fs%3D5%26n%3D10.result "$MM_URL/feeds/IPv4HC?s=5&n=10"
curl -# -k -o IPv4HC%3Fv%3Djson%26tr%3D1.result "$MM_URL/feeds/IPv4HC?v=json&tr=1"
curl -# -k -o IPv4HC%3Fv%3Djson-seq.result "$MM_URL/feeds/IPv4HC?v=json-seq"
curl -# -k -o IPv4HC%3Fv%3Dmwg.result "$MM_URL/feeds/IPv4HC?v=mwg"
curl -# -k -o IPv4HC%3Fv%3Dcsv%26f%3Dconfidence%26f%3Dsources%7Cfeeds%26f%3Dindicator%7Cclientip%26tr%3D1.result "$MM_URL/feeds/IPv4HC?v=csv&f=confidence&f=sources|feeds&f=indicator|clientip&tr=1"
curl -# -k -o URLHC.result "$MM_URL/feeds/URLHC"
curl -# -k -o URLHC%3Fs%3D5%26n%3D10.result "$MM_URL/feeds/URLHC?s=5&n=10"
curl -# -k -o URLHC%3Fv%3Djson%26tr%3D1.result "$MM_URL/feeds/URLHC?v=json&tr=1"
curl -# -k -o URLHC%3Fv%3Djson-seq.result "$MM_URL/feeds/URLHC?v=json-seq"
curl -# -k -o URLHC%3Fv%3Dmwg.result "$MM_URL/feeds/URLHC?v=mwg"
curl -# -k -o URLHC%3Fv%3Dcsv%26f%3Dconfidence%26f%3Dsources%7Cfeeds%26f%3Dindicator%7Curl.result "$MM_URL/feeds/URLHC?v=csv&f=confidence&f=sources|feeds&f=indicator|url"
curl -# -k -o URLHC%3Fv%3Dbluecoat.result "$MM_URL/feeds/URLHC?v=bluecoat"
curl -# -k -o URLHC%3Fv%3Dbluecoat%26cd%3Dtest.result "$MM_URL/feeds/URLHC?v=bluecoat&cd=test"
curl -# -k -o URLHC%3Fv%3Dpanosurl.result "$MM_URL/feeds/URLHC?v=panosurl"
curl -# -k -o URLHC%3Fv%3Dpanosurl%26sp%3D1.result "$MM_URL/feeds/URLHC?v=panosurl&sp=1"
curl -# -k -o URLHC%3Fv%3Dpanosurl%26di%3D1.result "$MM_URL/feeds/URLHC?v=panosurl&di=1"
curl -# -k -o DomainHC%3Fv%3Dcarbonblack.result "$MM_URL/feeds/DomainHC?v=carbonblack"
