# 설치 방법

## 1. 사전조건

* Oracle Java (Java8 이상) 설치되어 있어야 한다.
* [Marmot 서버](https://github.com/kwlee0220/marmot.server.dist)를 사용할 수 있어야 한다.
* [marmot.client.dist](https://github.com/kwlee0220/marmot.client.dist) 가 설치되어 있어야 한다.

## 2. 프로젝트 파일 다운로드 및 컴파일
`$HOME/marmot` 디렉토리를 만들어서 이동한다. 
<pre><code>$ cd $HOME/marmot</code></pre>

GitHub에서 `marmot.sample' 프로젝트를 download하고, 받은 zip 파일 (marmot.sample-master.zip)의
압축을 풀고, 디렉토리 이름을 `marmot.sample`로 변경한다.
* GitHub URL 주소: `https://github.com/kwlee0220/marmot.sample`
* 생성된  `marmot.sample` 디렉토리는 `$HOME/marmot/marmot.sample`에 위치한다.

생성된 디렉토리로 이동하여 컴파일을 시도한다.
<pre><code>$ cd marmot.sample
$ gradle assemble
</code></pre>

컴파일 도중 오류가 발생될 수 있는데 이경우 `$HOME/.gradle 디렉토리로 이동하여
다음과 같은 내용의 `gradle.properties` 파일을 생성한다.
<pre><code>distRepositoryDir=/home/xxx/marmot/dists
</code></pre>
여기서 `/home/xxx`는 사용자 홈디렉토리 경로명을 의미한다. 위 파일을 생성한 뒤 앞선 gradle 명령어를
사용하여 컴파일을 다시 시도한다.

Eclipse IDE를 이용하려는 경우 `eclipse` Gradle task를 수행시켜 Eclipse 프로젝트 import에
필요한 `.project` 파일과 `.classpath` 파일을 생성시킨다.
<pre><code>$ gradle eclipse</code></pre>

## 3. 샘플 설명

#### 3.1 Package: `basic`

Marmot client를 사용하는 기본적인 기능의 예제를 보여주는 샘플 프로그램들이다.
* `basic.SampleCatalog`
* `basic.SampleCreateDataSet`
* `basic.SampleFilter` 
* `basic.SampleUpdate` 
* `basic.SampleSort`
* `basic.SampleAggregate`
* `basic.SampleAssignUid`
* `basic.SampleExecuteLocally` 

#### 3.2 Package: `geom`

Marmot이 제공하는 기능 중에 공간 정보를 사용하는 예제를 보여주는 샘플 프로그램들이다.

#### 3.3 Package: `geom.advanced`

Marmot이 제공하는 기능 중에 공간 정보를 활용하는 고급 분석 기능을 활용하는 예제를 보여주는 샘플 프로그램들이다.

#### 3.4 Package: `carloc`

나비콜에서 제공하는 택시 주행 로그를 활용하는 예제 샘플 프로그램들이다.
* `carloc.CalcHeatMap`: 택시 주행 로그를 서울시 영역 내의 사각 그리드를 기준으로 분류하는 예제 프로그램.
* `carloc.FindBestRoadsForPickup`: 시간대 별로 **공차** 택시가 가장 많은 도로 구간을 분석하는 예제 프로그램.
* `carloc.FindHotHospitals`: 서울 시내 종합병원 근처 50m 이내에서 택시 승하차 횟수를 수집하는 예제 프로그램.
* `carloc.FindHotTaxiPlaces`: 서울 시내 읍면동별로 택시 승하차 횟수를 수집하는 예제 프로그램.
* `carloc.FindLongTaxiTravels`: 택시 주행 경로 데이터 중에서 승차 경로의 전체 길이 별로 나열하는 예제 프로그램.
* `carloc.FindPassingStation`: 택시 주행 경로 데이터 중에서 **사당역**주변 100m 이내로 승차 택시 경로가 겹치는
	모든 경로를 찾는 예제 프로그램
