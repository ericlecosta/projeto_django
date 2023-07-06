from django.shortcuts import render
import csv

# Create your views here.

def home(request):
    return render(request,'testes/home.html')

def conexoes(request):
    csv_fp = open('conexoes.csv', 'r')
    reader = csv.DictReader(csv_fp,delimiter=';')
    headers = [col for col in reader.fieldnames]
    out = [row for row in reader]
    return render(request,'testes/conexao.html',{'data' : out})