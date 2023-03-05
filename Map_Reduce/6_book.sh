#! /bin/bash

chmod +x 6_book.sh

# Merge the links of the group in 'books.txt'
# Everyone in the group save a txt with ther books as 'books_name.txt'
cat books_* > books.txt

# Save the books with the shell file of point 1 of the exam
./1_*.sh books.txt

# To run this file, use a terminal command like this

# > ./6_book.sh