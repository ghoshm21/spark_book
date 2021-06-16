A=0
while IFS= read -r LINE ; do
  printf '%s\n' "$LINE" > newfile$A
  (( A++ ))
done < "wiki_eng_articles.txt"
