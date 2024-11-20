package objectnode

import (
	"github.com/cubefs/cubefs/util/log"
	"runtime"
)

type ReplicateFileInfo struct {
	FileInfo  FSFileInfo
	TargetIds []string
}

type ReplicationState struct {
	replicationCh chan ReplicateFileInfo
}

func (r *ReplicationState) queueReplicaTask(info ReplicateFileInfo) {
	if r == nil {
		return
	}
	select {
	case r.replicationCh <- info:
	default:
		log.LogErrorf("discard file (inode:%v, path:%v) when async replication, because the replication chan is full ",
			info.FileInfo.Inode, info.FileInfo.Path)
	}
}

func NewReplicationState(closeCh chan struct{}, volume *Volume) *ReplicationState {
	rs := &ReplicationState{
		replicationCh: make(chan ReplicateFileInfo, 10000),
	}

	// add background groutines to deal with replication tasks
	workerNum := runtime.GOMAXPROCS(0) / 4
	if workerNum == 0 {
		workerNum = 1
	}
	for i := 0; i < workerNum; i++ {
		go func() {
			for {
				select {
				case <-closeCh:
					return
				case replicateFileInfo, ok := <-rs.replicationCh:
					if !ok {
						// chan closed
						return
					}
					f := replicateFileInfo.FileInfo
					if attrInfo, err := volume.mw.XAttrGetAll_ll(f.Inode); err == nil {
						volume.replicateObject(&f, attrInfo.XAttrs, replicateFileInfo.TargetIds)
					} else {
						log.LogErrorf("err when asynchronous replicate in background groutine: volume(%v) path(%v) inode(%v) err(%v)",
							volume.name, f.Path, f.Inode, err)
					}
				}
			}
		}()
	}

	// cleanup when close
	go func() {
		select {
		case <-closeCh:
		}

		close(rs.replicationCh)
	}()

	return rs
}
