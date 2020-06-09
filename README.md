# ZUT Big Data

> Zadania z Big Data studia magisterskie

<div align="center"><img src="https://i.imgur.com/DNnWamo.png" width="50%"></div>

## Zadanie 2

W ramach zadania należy:

1. Utworzyć projekt w języku Java, korzystający z biblioteki Spark (można użyć dostosować przykładowy projekt https://github.com/bmalach/spark-proj-template)
2. Dołączyć do projektu plik z danymi pochodzący ze strony https://grouplens.org/datasets/movielens/ zawierający oceny filmów
3. Napisać kod, który sprawdzi ile razy każdy film został oceniony oraz wyświetlić te informacje w postaci posortowanej wg liczby ocen.
4. Przesłać zadanie Prowadzącemu poprzez platformę e-edukacja.zut.edu.pl 

Etapy programu:

1. Wczytanie pliku CSV jako kolekcji linii tekstowych
2. Zmapowanie kolekcji z liniami tekstu na kolekcję z tablicami rozbitych wartości tekstowych wg symbolu przecinka
3. Zmapowanie kolekcji z tablicami wartości tekstowych na kolekcję dwójek klucz-wartość (id filmu, 1)
4. Zredukowanie kolekcji dwójek wg klucza (id filmu) w taki sposób, aby przy każdej redukowanej liczona była suma wartości
5. Zmapowanie kolekcji wynikowej z etapu 4 na kolekcję dwójek, w której zamieniono klucz z wartością
6. Posortowanie kolekcji wynikowej z etapu 5 wg klucza

## Zadanie 3

W ramach zadania należy:

1. Utworzyć projekt w języku Java, korzystający z biblioteki Spark (można użyć dostosować przykładowy projekt https://github.com/bmalach/spark-proj-template)
2. Dołączyć do projektu pliki z danymi pochodzące ze strony https://grouplens.org/datasets/movielens/ zawierające oceny filmów oraz tytuły filmów
3. Napisać kod, który: 
    * wczyta pliki z danymi o ocenach i tytułach do odrębnych kolekcji RDD
    * policzy średnią ocen dla każdego filmu
    * połączy kolekcję zawierającą średnie oceny z kolekcją zawierającą tytuły filmów
    * posortuje wyniki wg średniej ocen 
    * wyświetli średnią ocenę dla każdego filmu używając jego tytułu
4. ~~Umieścić pliki z danymi w systemie plików FDFS na wirtualnej instancji ze środowiskiem Hadoop~~
5. ~~Uruchomić zbudowaną wersję opracowanego programu (plik .jar) na instancji wirtualnej, tak aby korzystała z plików z danymi w HDFS~~
6. Przesłać zadanie (kod źródłowy) Prowadzącemu poprzez platformę e-edukacja.zut.edu.pl 

## Zadanie 4

W ramach zadania należy:

1. Utworzyć projekt w języku Java, korzystający z biblioteki Spark (można dostosować przykładowy projekt https://github.com/bmalach/spark-proj-template)
2. Dołączyć do projektu pliki z danymi pochodzące ze strony https://grouplens.org/datasets/movielens/ zawierające dane z serwisu filmowego (w wariancie małym).
3. Napisać kod, który wykona następujące analizy: 
    * policzy i wyświetli informacje o liczbie przypisanych gatunków filmowych do każdego filmu (na podstawie pliku movies.csv)
    * na podstawie kolumny z danymi o gatunkach filmowych w pliku z filmami utworzy listę wszystkich gatunków (bez powtórzeń) i wyświetli ją posortowaną wg popularności (liczby filmów przypisanych do kategorii)
    * stworzy listę tagów przypisanych przez użytkowników poszczególnym filmom (na podstawie pliku tags.csv). Policzy ile tagów zostało przypisanych do każdego filmu. Wyświetli te dane używając tytułów filmów.
    * znajdzie i wyświetli te tytuły filmów, które mają przypisanych więcej tagów niż kategorii.
4. Przesłać zadanie do oceny korzystając z opcji "prześlij zadanie" platformy e-edukacja.zut.edu.pl

## Zadanie 5

W ramach zadania należy wykorzystać algorytm ALS z biblioteki Spark MLlib do wykonania mechanizmu rekomendacji. Do tego celu należy użyć dane testowe Movie Lens Database https://grouplens.org/datasets/movielens/

Rekomendacja powinna polegać, na wygenerowaniu dla każdego filmu, który był oceniony ponad 15 razy, trzech sugestii wskazujących użytkowników możliwie najbardziej zainteresowanych danym tytułem.

Najważniejsze etapy zadania:

1. Dodanie biblioteki spark-mllib do zależności projektu Java (plik pom.xml)
2. Wczytanie pliku ratings.csv oraz movies.csv
3. Zmapowanie danych o ocenach filmów do postaci akceptowanej przez klasę implementującą algorytm ALS
4. "Wytrenowanie" modelu rekomendującego
5. Policzenie ile razy każdy film był oceniony
6. Wygenerowanie rekomendacji (3 sugerowanych użytkowników dla każdego filmu)
7. Połączenie rekomendacji z licznikiem filmów
8. Filtrowanie rekomendacji wg kryterium min. liczby ocen (np. przynajmniej 10)
9. Połączenie rekomendacji z tytułami filmów
10. Wyświetlenie końcowej rekomendacji

## Zadanie 6

W ramach zadania należy wykonać następujące czynności:

1. Ładowanie obiektów Data Frame z plików tekstowych
    * załadować pliki movies.csv oraz movies.json od oddzielnych obiektów DataFrame
    * wyświetlić ich zawartość (metoda: show()) oraz opis schematu (metoda: printSchema())
2. Zadawanie zapytań w języku SQL
    * utworzyć widoki SQL dla obu utworzonych wcześniej obiektów DataFrame
    * wysłać do widoków zapytanie wg schematu: SELECT * FROM widok WHERE movieId > 500 ORDER BY movieId DESC
    * wyświetlić rezultaty obu zapytań i porównać je ze sobą.
3. Operacje łączenia i agregacji
    * wczytać plik z danymi o ocenach filmów (ratings.csv)
    * wykonać operację grupowania ocen wg identyfikatora filmu oraz policzenia wystąpień w ramach grupy
    * połączyć wynik grupowania i liczenia ze zbiorem danych o filmach
    * wyświetlić tytuły filmów wg liczby ocen
4. Tworzenie obiektów typu DataSet
    * utworzyć klasę obiektów (np. Movie) służącą do zmapowania danych pochodzących z movies.json
    * wczytać dane do obiektu typu Dataset<Movie>
5, Wczytanie danych o filmach i ocenach z serwera MySql

do skutecznego połączenia potrzebne jest podanie parametru ustawiającego strefę czasową, przykład: prop.setProperty("serverTimezone","UTC");
Uwaga! ostatni krok wymaga dołączenia do projektu sterownika MySql: https://mvnrepository.com/artifact/mysql/mysql-connector-java/8.0.18

Projekt powinien mieć także dołączoną bibliotekę z komponentem Spark SQL: https://mvnrepository.com/artifact/org.apache.spark/spark-sql_2.12

## Zadanie 7

W ramach zadania należy wykonać analizy korzystając z przykładowych danych o zamówieniach w sklepie:

https://www.instacart.com/datasets/grocery-shopping-2017

Opis danych: https://gist.github.com/jeremystan/c3b39d947d9b88b3ccff3147dbcf6c6b

Do realizacji należy wykorzystać Dataset API. 

https://spark.apache.org/docs/latest/sql-getting-started.html#untyped-dataset-operations-aka-dataframe-operations

https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/Dataset.html

Skorzystanie z Dataset API wymaga dołączenia do projektu biblioteki komponentu Spark SQL:
https://mvnrepository.com/artifact/org.apache.spark/spark-sql_2.12/2.4.5

Warto także dołączyć w swoim pliku import statyczny do funkcji związanych z API spark sql:

import static org.apache.spark.sql.functions.*;
Analizy do wykonania:

1) Policzenie liczby zamówień wg godziny ich wykonania

Rezultat do uzyskania:

```
+-----------------+------+
|order_hour_of_day| count|
+-----------------+------+
|               00| 22758|
|               01| 12398|
|               02|  7539|
|               03|  5474|
|               04|  5527|
|               05|  9569|
|               06| 30529|
|               07| 91868|
|               08|178201|
|               09|257812|
|               10|288418|
|               11|284728|
|               12|272841|
|               13|277999|
|               14|283042|
|               15|283639|
|               16|272553|
|               17|228795|
|               18|182912|
|               19|140569|
|               20|104292|
|               21| 78109|
|               22| 61468|
|               23| 40043|
+-----------------+------+
```
2) Policzenie zamówień wg dnia tygodnia ich wykonania (wyświetlenie w kolejności rosnącej). 

Do wyświetlenia wyniku należy użyć nazw dni. W tym celu należy stworzyć własny Dataset zawierający numery i nazwy dni tygodnia, a następnie połączyć go z wynikiem zliczania zamówień (proszę jako pierwszy dzień tygodnia przyjąć niedzielę).

Rezultat do uzyskania:

```
+------------+------+
|       nazwa| count|
+------------+------+
|    czwartek|426339|
|       środa|436972|
|      sobota|448761|
|      piątek|453368|
|      wtorek|467260|
|poniedziałek|587478|
|   niedziela|600905|
+------------+------+
```

3) Policzenie liczby oraz procentowego rozkładu produktów wg działu sklepu (wyświetlenie w kolejności rosnącej)

Rezultat do uzyskania:

```
+---------------+-----+-------------------+
|department     |count|procent            |
+---------------+-----+-------------------+
|bulk           |38   |0.07647721783931734|
|other          |548  |1.1028819835775239 |
|meat seafood   |907  |1.8253904363226534 |
|pets           |972  |1.9562067299951698 |
|alcohol        |1054 |2.12123651585896   |
|babies         |1081 |2.175575591692159  |
|breakfast      |1115 |2.244002576074706  |
|international  |1139 |2.2923039768153277 |
|missing        |1258 |2.5317984221542424 |
|deli           |1322 |2.6606021574625665 |
|bakery         |1516 |3.0510384801159236 |
|produce        |1684 |3.3891482853002737 |
|dry goods pasta|1858 |3.7393334406697796 |
|canned goods   |2092 |4.210272097890839  |
|household      |3085 |6.208742553534052  |
|dairy eggs     |3449 |6.941313798100144  |
|frozen         |4007 |8.064321365319595  |
|beverages      |4365 |8.784817259700532  |
|pantry         |5371 |10.809450974078247 |
|snacks         |6264 |12.606665593302205 |
|personal care  |6563 |13.208420544195782 |
+---------------+-----+-------------------+

```
Uwaga! Proszę pamiętać o każdorazowym wyświetlaniu wszystkich wyników (domyślnie wyświetlane jest pierwsze 20) oraz o właściwym sortowaniu.

## Zadanie 8

Celem zadania jest wykonanie analizy wybranego zestawu stron z serwisu Wikipedia polegającego na obliczeniu ich istotności na podstawie wzajemnych powiązań (algorytm PageRank).

W tym celu należy: 

* wyeksportować do formatu XML wybrany zakres stron z serwisu Wikipedia za pomocą narzędzia https://en.wikipedia.org/wiki/Special:Export
* utworzyć na bazie szablonu projekt aplikacji Spark
* wczytać w aplikacji wygenerowany plik XML do kolekcji typu Dataset (importując należy ograniczyć się wyłącznie do węzłów `<page>` znajdujących się w pliku XML z Wikipedii)
* przy pomocy mapowania płaskiego utworzyć kolekcję powiązań każdego artykułu z odnośnikami znajdującymi się w jego tekście.
* W tym celu należy utworzyć element reprezentujący krawędź grafu dla każdej pary składającej się z tytułu aktualnego artykułu (węzeł `<title>`) oraz znalezionego odnośnika w jego treści (zawartość węzła `<text>`). Krawędzie grafu powinny reprezentować obiekty samodzielnie zdefiniowanej klasy. Do znalezienia odsyłaczy w treści artykułu należy użyć wyrażenia regularnego (przykład: ```"\\[\\[([.[^\\]]]+)\\]\\]"```).
* Krawędzi grafu nie należy tworzyć dla odsyłaczy typu: Category:, File:, Image:
* na podstawie kolekcji reprezentującej krawędzie grafu utworzyć graf typu GraphFrame
* dla grafu wykonać obliczenie PageRank (proszę nie ustawiać dużych wartości parametru maxIter, gdyż obliczenia mogą być bardzo długie).

Potrzebne zależności:

https://mvnrepository.com/artifact/org.apache.spark/spark-sql
https://mvnrepository.com/artifact/org.apache.spark/spark-graphx
https://mvnrepository.com/artifact/graphframes/graphframes
https://mvnrepository.com/artifact/com.databricks/spark-xml
Dołączenie biblioteki GraphFrames wymaga dodania w pliku konfiguracyjnym pom.xml adresu repozytorium, z którego może ona być pobrana:

```
<repositories>
   <repository>
      <id>SparkPackages</id>
      <url>https://dl.bintray.com/spark-packages/maven/</url>
   </repository>
</repositories>
```
