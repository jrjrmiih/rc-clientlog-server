ssh logcol@$1 "rm /home/logcol/library/flume-1.8.0/lib/rc-flume-httpsource-1.0.jar"
scp ./httpsource/target/rc-flume-httpsource-1.0.jar logcol@$1:/home/logcol/library/flume-1.8.0/lib/
ssh logcol@$1 "rm /home/logcol/library/flume-1.8.0/lib/rc-flume-hbasesink-1.0.jar"
scp ./hbasesink/target/rc-flume-hbasesink-1.0.jar logcol@$1:/home/logcol/library/flume-1.8.0/lib/
