cp input.txt data.txt

for i in {1..5}; do
  cat data.txt \
  | python3 mapper_auth.py \
  | sort \
  | python3 reducer_auth.py \
  | python3 mapper_hub.py \
  | sort \
  | python3 reducer_hub.py > tmp.txt
  mv tmp.txt data.txt
done
