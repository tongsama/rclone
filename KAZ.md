# KAZ.md: kaz 改修の移植の手引き

このファイルは kaz のブランチ（`1.75.2-improve-kaz` など）にだけ置く。upstream の rclone を新しい版に上げるとき、
kaz の改修を移し直す手順と、見るべき場所をまとめる。背景は調査リポジトリの
`docs/inspection/rclone_v1752_port/2026-10-13/port-investigation-ja.md` を参照。

## 1. 機能ごとのフック点と、頼っている upstream の性質

### lookup（`--kaz-vfs-lookup-by-path`、vfs）
- フック点: `Dir.stat`（キャッシュに無い名前を一覧せず `Dir.lookup` へ）、`Dir.statCachedNormalized`、
  `Dir.checkName`（unsafe な名前の拒否。lookup でも必ず通す）、`VFS.OpenFile` の O_TRUNC の近道（`kazCreateNoLookup`）、
  `File.kazObjectGone`（file.go）、`Dir.cacheCleanup`／`ForgetAll`（lookup のエントリの期限切れ）。
  実装は `vfs/dir_kaz_lookup.go`。
- 頼っている性質: `Dir.Create` と同じ名前の検査（GHSA 修正 2f43b99ab）、ディレクトリのキャッシュの排他（`d.mu`）、
  `NewObject` が完全一致で引くこと。
- **backend の前提**: `Dir.lookup` はディレクトリを、`NewObject` が `fs.ErrorIsDir` を返すことでしか見分けない
  （`vfs/dir_kaz_lookup.go`）。本番は drive の性質に頼る（`backend/drive/drive.go` の `newObjectWithExportInfo` が
  `driveFolderType` で `ErrorIsDir` を返す。`NewObject` 末尾が `/` のときも同じ。1717／1759 行付近）。
  memory やバケット型の backend はこの前提を満たさないので、lookup モードの対象外。drive にこの性質を守るテストは
  今は無い（あれば `TestKazLookup*` のように足す）。
- テスト: `TestKazLookupUnsafeName`、`TestKazPathTraversalLookup`（cmd/serve/nfs）、`TestKazLookup*`、
  `TestKazCreateTrunc*`、`TestKazRemove*`、`TestKazStatCachedNormalizedDuplicate`（すべて vfs、nfs は上記1件）。

### persist（`--kaz-s3-persist-metadata`、`--drive-kaz-properties`）
- フック点: `tenant.storeMeta`／`getMeta`（オンのときは LRU に入れず `kazStoreMeta`／`kazUserMeta`）、
  `multipartUpload.createSink()`（`sink()` と `close()` から呼ばれる。最初の part の sink を作るときに `kazSetUploadMetadata` で
  メタデータを渡す。CreateMultipartUpload では backend に何も作らない）、`vfs` の `WriteFileHandle` が
  メタデータを Rcat に渡すこと（`vfs/kaz_upload_metadata.go`、`write.go`）、drive の upload への properties（`backend/drive/kaz_properties.go`）。
  `PutObject`／`CopyObject`／`DeleteObject` の `backend.go` の分岐。
- 頼っている性質: `multipartUpload` が最初の part で Create すること（`--multipart-max-uploads` の前提）、
  part が逆順や0個でも sink が開けること、`WriteFileHandle` の Rcat がメタデータを受け取れること。
- テスト: `TestKazPersist*`（`TestKazPersistMultipart`、`...OutOfOrder`、`...NoParts`、`...CreateMakesNothing`、
  `...CopySameKey`）、`TestKazUploadMetadata*`（vfs）、`TestKazFieldsIncludeProperties` ほか（drive）。

### cancel-get（`--kaz-s3-cancel-get-on-disconnect`）
- フック点: `GetObject` の本文の組み立て（`backend.go` で `kazOpenObject` を呼ぶ）、`etagHash`（ETag のハッシュの選択）。
  実装は `cmd/serve/s3/kaz_get.go`。
- 頼っている性質: リクエストの ctx がクライアント切断で cancel されること、`--vfs-cache-mode off`。
- テスト: `TestKazCancelGetOnDisconnect*`、`TestKazGetObjectDeletedElsewhere`。

### 一覧（`--kaz-s3-list-by-key-order`）
- 現状は受け付けるだけの no-op。upstream の `lister`（ee40507d3）がキー順に一覧する。
- フック点: `lister.list()` の先頭の ctx の確認（切断で次のディレクトリへ進まない）、`listPage` 末尾の `kaz list:` のデバッグ行。
- テスト: `TestKazListStopsOnCancel`、`TestKazListMatchesSortedWalk`、`TestKazList*`。

### エラー（Stat の失敗を 404 にしない）
- 方針: ENOENT のときだけ NoSuchKey／NoSuchBucket、他は 500。`cmd/serve/s3/kaz_errors.go` の
  `bucketStatError`／`keyStatError` を使う。呼び出し箇所（`grep -n 'StatError(' cmd/serve/s3/*.go`）:
  backend.go の ListBucket、HeadObject、GetObject、PutObject、deleteObject、DeleteBucket、CopyObject と、
  multipart.go の CreateMultipartUpload。
- 新しい Stat の経路が増えたら `grep -n 'NotFound(' cmd/serve/s3/*.go` で、`gofakes3.BucketNotFound`／`KeyNotFound` を
  直接返していないかを確かめる（許されるのは kaz_errors.go と、パスが無いと確定した箇所のみ）。
- テスト: `TestKazStatErrorsAreNot404`、`TestKazKeyStatErrorIsNot404`、`TestKazBucketStatErrorsElsewhereAreNot404`。

## 2. 次の版への移し方

1. 新しい upstream タグ（`vX.Y.Z`）から `X.Y.Z-improve-kaz` と、機能ごとの作業ブランチ
   （lookup → persist → cancel-get → list の順に積む）を切る。
2. 旧版の kaz の commit を機能ごとに cherry-pick（`-x` なし）。衝突したらフック点の関数の形の変化から直す。
3. upstream の差分を確認する:
   `git log --oneline <旧>..<新> -- cmd/serve/s3 vfs backend/drive` で、GHSA 付きのコミットと、
   上のフック点の関数（`Dir.stat`、`checkName`、`OpenFile`、`storeMeta`、`createSink`、`GetObject`、`lister` など）の変更を読む。
   セキュリティ修正が kaz の経路を通らないなら、先にテストを足す（例: Task 1 の `TestKazLookupUnsafeName`）。
4. upstream のテストを kaz モードで流す（下記）。表に無いテストが落ちたら、修正の迂回の可能性があるので理由を調べる。
5. 通常のテスト（`-tags` なし）、`-race`、lint を流してから `X.Y.Z-improve-kaz` へ merge する。

### upstream のテストを lookup モードで流す

`vfs`、`cmd/serve/nfs`、`cmd/serve/s3` の `kaz_mode_test.go`（build tag `kazlookup`）が
`vfscommon.Opt.KazLookupByPath = true` を既定にする。

```bash
export GOTOOLCHAIN=go1.26.9
# vfs と nfs
go test -tags kazlookup ./vfs/ ./cmd/serve/nfs/ -race -count=1 \
  -skip '^(TestDirForgetAll|TestDirForgetPath|TestCaseSensitivity|TestUnicodeNormalization)$'
# s3 (1): memory の副テストだけを外す（Local の副テストは流す）
go test -tags kazlookup ./cmd/serve/s3/ -race -count=1 \
  -run '^(TestMultipartAbort|TestMultipartAbortPreservesExisting|TestMultipartOverwrite|TestPutObjectFailurePreservesExisting|TestPutObjectFailureNewKey)$' -skip '/Memory$'
# s3 (2): TestPutObjectFailureCached は <mode>/Memory の順
go test -tags kazlookup ./cmd/serve/s3/ -race -count=1 -run '^TestPutObjectFailureCached$' -skip '//Memory$'
# s3 (3): 残り。(1)(2) の分は上で流すので外す。memory だけのテストと TestS3 系も外す
go test -tags kazlookup ./cmd/serve/s3/ -race -count=1 \
  -skip '^(TestS3|TestS3Minio|TestMultipartNoServerSideMove|TestMultipartCacheModeWritesNoServerSideMove|TestMultipartAbort|TestMultipartAbortPreservesExisting|TestMultipartOverwrite|TestPutObjectFailurePreservesExisting|TestPutObjectFailureNewKey|TestPutObjectFailureCached)$'
# s3 (4): backend/s3 の統合テスト。FsEncoding だけを外す（-sub-run は servetest の flag）
go test -tags kazlookup -run '^TestS3$' ./cmd/serve/s3/ -count=1 \
  -args -sub-run 'TestIntegration//^([^F]|F[^s]|Fs[^E]|FsE[^n])'
```

`TestS3Minio` は docker が要るので除く（lookup とは無関係）。`-skip` は `/` を `|` の選択肢に入れられないので、
コマンドを分けている。上の表のテストだけが、lookup モードの仕様上の違いで落ちてよい。

| テスト | 落ちる理由 |
|---|---|
| `TestDirForgetAll`、`TestDirForgetPath`（vfs） | lookup モードの Stat はディレクトリを一覧しないので、item がキャッシュに載る前提が成り立たない |
| `TestCaseSensitivity`、`TestUnicodeNormalization`（vfs） | キャッシュに無い名前は NewObject で完全一致を引くので、大文字小文字・Unicode の正規化での一致を探さない（本番は JuiceFS の ASCII の key だけで影響なし） |
| s3 の `Memory` の副テスト: `TestMultipartAbort/Memory`、`TestMultipartAbortPreservesExisting/Memory`、`TestMultipartOverwrite/Memory`、`TestPutObjectFailurePreservesExisting/Memory/*`、`TestPutObjectFailureNewKey/Memory/*`、`TestPutObjectFailureCached/<mode>/Memory`、と memory だけの `TestMultipartNoServerSideMove`、`TestMultipartCacheModeWritesNoServerSideMove` | memory backend は `NewObject` がバケットに `ErrorIsDir` を返さず（not found）、上の「backend の前提」を満たさないので lookup モードの対象外。NoSuchBucket になる |
| `TestS3` の `TestIntegration/FsMkdir/FsEncoding`（s3） | 観察: 一覧で返る leaf が `␀‛␁‛␂…` のように、local の quote 文字 `‛` が残った名前になり、期待の `␀␁␂…` と合わない（二重に符号化されたように見える）。後続の `FsPutFiles` などは後始末できない残りで連鎖して落ち、10分の timeout になる（FsEncoding を外すと全て通る）。lookup でなくても起きるかは未検証。本番の key は ASCII／日本語で影響なし |

この表に無いテストが落ちたら、仕様差か、upstream の修正の迂回かを調べる。仕様差だけを表に足す。
