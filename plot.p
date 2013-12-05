set terminal postscript landscape color enhanced
set grid
set title "Execution Time v/s Tables"
set xlabel "Num Of Tables"
set ylabel "Execution Time (Secs)"
plot "plot.txt" using 1:2 with linespoint
set output "result.ps" 
# or any other filename
replot
unset output
