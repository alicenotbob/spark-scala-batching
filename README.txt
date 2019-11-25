1) Required screenshots available in ./screnshots/

2) open envvars.sh and customize all variables

3) App sources + test sources available in ./src/

5) To run app follow the next steps:
 - execute in current folder "sbt clean compile package"
 - to run app on yarn, execute run_app_on_yarn.sh.
   Since it is hard to make screenshot of execution, there are the log of application runned on yarn (yarn_app_log.txt).
   In this log you can see required results:
    
      line 1371:
        +-------------+-------------------------------------+-------+--------------+--------------------------------------------------------------------------+-----------+------------+-------+
        |id           |name                                 |country|city          |address                                                                   |latitude   |longitude   |geohash|
        +-------------+-------------------------------------+-------+--------------+--------------------------------------------------------------------------+-----------+------------+-------+
        |2302102470656|Park International Hotel             |GB     |London        |117 129 Cromwell Road Kensington and Chelsea London SW7 4DS United Kingdom|51.4945144 |-0.1866172  |gcpug  |
        |2662879723520|Okko Hotels Paris Porte De Versailles|FR     |Paris         |2 Rue du Colonel Pierre Avia 15th arr 75015 Paris France                  |48.8327729 |2.2787597   |u09tg  |
        |3100966387715|Vincci Gala                          |ES     |Barcelona     |Ronda Sant Pere 32 Eixample 08010 Barcelona Spain                         |41.3895263 |2.1747136   |sp3e3  |
        |197568495617 |Fairfield Inn & Suites Peoria East   |US     |East Peoria   |200 Eastlight Ct                                                          |40.67221051|-89.57642544|dp0r3  |
        |3058016714753|Hazlitt s                            |GB     |London        |6 Frith Street Soho Westminster Borough London W1D 3JA United Kingdom     |51.5143447 |-0.1318157  |gcpvj  |
        |206158430210 |The Litchfield Inn                   |US     |Pawleys Island|1 Norris Dr                                                               |33.470477  |-79.097809  |djzy2  |
        +-------------+-------------------------------------+-------+--------------+--------------------------------------------------------------------------+-----------+------------+-------+
      
      line 5884:
        +-------+--------------+-----+
        |country|city          |count|
        +-------+--------------+-----+
        |US     |Sweetwater    |981  |
        |US     |Selma         |992  |
        |US     |Fond Du Lac   |953  |
        |US     |San Francisco |3073 |
        |US     |Jeffersonville|1076 |
        |US     |Longville     |1087 |
        |US     |Cave Spring   |1046 |
        |US     |Ironwood      |1015 |
        |US     |Stockton      |3056 |
        |US     |Jellico       |1988 |
        |US     |Buena Park    |1021 |
        |US     |Forest Grove  |933  |
        |FR     |Paris 06      |983  |
        |US     |Bristol       |1002 |
        |US     |Richmond      |1025 |
        |US     |Young         |1028 |
        |US     |Catonsville   |942  |
        |US     |Hauppauge     |995  |
        |US     |Bellmawr      |1005 |
        |US     |Crystal Bay   |990  |
        +-------+--------------+-----+
