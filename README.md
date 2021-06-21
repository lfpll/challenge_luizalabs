# Challenge_luizalabs

A maneira de rodar é bem simples é um docker file que já possui todos os arquivos para ser visualizado.

Rode:

    docker build . -t luizalabs


E então:

    docker run -it --rm -p 8888:8888 luizalabs

Ou simplismente rode os jobs localmente, eles são simplificados e não usam nenhum excesso de complexidade como udfs e afins.

Os 2 notebooks ficaram disponiveis no docker no endereço [http://127.0.0.1:8888/](http://127.0.0.1:8888/).

 - First Challenge.ipynb
 - Second Challenge.ipynb
    
Além disso dentro das pastas também existem os arquivos dos "jobs", sem nenhum dinamismo e sem paths relativos apenas para mostra do conhecimento.

Os resultados estão dentro de cada pasta em um arquivo **resultado.csv**
