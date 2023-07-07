from django.shortcuts import render
import csv

# Create your views

def home(request):
    return render(request,'testes/home.html')

def conexoes(request):
    csv_fp = open('conexoes.csv', 'r')
    reader = csv.DictReader(csv_fp,delimiter=';')
    headers = [col for col in reader.fieldnames]
    out = [row for row in reader]
    return render(request,'testes/conexao.html',{'data' : out})

def conf_conexao(request):
    csv_fp = open('conexoes.csv', 'r')
    reader = csv.DictReader(csv_fp,delimiter=';')
    out = [row for row in reader]
    nome_banco = 'teste'
    #for reg in out:
        #banco_post = "'" + reg.get("no_dados") + "'"
        #if  request.POST.get("'" + reg.get("no_dados") + "'") != None:
            #nome_banco = request.POST.get("'" + reg.get("no_dados") + "'")
    nome_banco = request.POST.get('sinan')
    return render(request,'testes/config_con.html',{'banco' : nome_banco})
