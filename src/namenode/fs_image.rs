// edit log is a transaction log that enables namenode to recover state upon restart
// when the namenode starts, it reads the latest consistent snapshot of the filesystem from the fs-image saved to disk.

// edit log also logs every change made to file-system metadata
