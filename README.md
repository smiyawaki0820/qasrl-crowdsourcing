# qasrl-crowdsourcing

Repository for QA-SRL tools, particularly for interaction and crowdsourcing.

## Contents

リポジトリ概要

 * `qasrl`: QA-SRL における `validating/interpreting/autocompleting`
 * `qasrl-crowd`: QA-SRL のデータを MTurk でクラウドソーシングするための UI とサーバコード
 * `qasrl-crowd-example`: クラウドソーシングパイプラインを自己のデータに使用するための sbt プロジェクト。

## Usage
- nlpdata と spacro の依存関係がダウンロードされ、ローカルの Ivy キャッシュにパブリッシュされる
- Wikitionary のデータを datasets/wikitionary にダウンロード

```bash
bash script/setup.sh
```

### Autocomplete
- オートコンプリート機能がどのように動作するかを知るために
- 質問のプレフィックス (または質問全体) を入力すると、自動補完のフィードバックが表示される

```bash
bash scripts/run_autocomplete_example.sh
```

- オートコンプリートやその他の機能を自分のコードで使用したい場合は、`scripts/autocomplete_example.scala` で使用例を確認してください。


### Crowdsourcing

- クラウドソーシングのパイプラインを起動し、UI のプレビューを見るには、`scripts/run_crowd_example.sh` を実行し、ブラウザで http://localhost:8888/task/generation/preview にアクセスしてください。
- MTurk での動作設定は、[こちら](https://github.com/uwnlp/qamr/tree/master/code) を参考にしてください。
- 主なエントリーポイントをたどるには：

   * sbt コンソールを始めるために [`scripts/crowd_example.scala`](https://github.com/julianmichael/qasrl-crowdsourcing/blob/master/scripts/crowd_example.scala) を実行
   * `qasrl-crowd-example` で定義された [AnnotationSetup](https://github.com/julianmichael/qasrl-crowdsourcing/blob/master/qasrl-crowd-example/jvm/src/main/scala/example/AnnotationSetup.scala) オブジェクトを作成し、クラウドソーシングパイプラインに必要な様々なデータとリソースを組み立てる
   * データのアップロード・ダウンロード、およびワーカーの評価を行うためのウェブサービス と MTurk インターフェースを作成する [QASRLAnnotationPipeline](https://github.com/julianmichael/qasrl-crowdsourcing/blob/master/qasrl-crowd/jvm/src/main/scala/qasrl/crowd/QASRLAnnotationPipeline.scala) オブジェクトが作成される。
   * 最後に、QASRLAnnotationPipeline オブジェクトに `start()` と伝えると、クラウドソーシングのタスクが開始される。
 
詳細は QAMR の[仕様書](https://github.com/uwnlp/qamr/tree/master/code)をご覧ください。

### Misc

最後に、このプロジェクトを自分のプロジェクトで使用するには、ソースの依存関係として追加するか、または
`sbt publishLocal` を実行して、目的のプロジェクトにマネージド依存関係として追加します。
