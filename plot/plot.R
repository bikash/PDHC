

pdf('/Users/bikash/Dropbox/purude/hadoopfailure/paper/LatexPaper/img/executiontime.pdf', bg = "white")
x <- c(0.5, 1.0, 1.5, 2.0)
#x <- c(0.5, 1.0, 1.5, 2)
y <- c(33.25,  33.56, 33.95, 34.15)
y1 <- c( 32.01, 32.15, 32.25, 33.01)
y2 <- c(30.93, 31.24, 31.51, 32.03)
plot(NULL, xlim=c(1,4), ylim=c(30,35), xlab="Data size", ylab="Execution Time",xaxt="n"  )
lines(y, lwd=2, col="red", lty = 2)
points(y, cex=1, col="red", lty=2, pch=18 )
axis(1, at=1:4, labels=x)
lines(y1, lwd=2, col="green", lty = 3)
points(y1,  cex=1, col="green", lty=3, pch=19)
lines(y2, lwd=2, col="blue", pch=23, lty=1)
points(y2, cex=1, col="blue", lty=1, pch=20 )
grid(14, 15, lty = 2)
legend("topleft",  legend=c("64MB", "128MB", "512MB" ) , cex=0.8, lty=1:3, pc=18:20, col=c("red","green","blue") )
dev.off()




