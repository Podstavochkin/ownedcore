#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import codecs

# Читаем файл с правильной кодировкой
with codecs.open('lightweight_charts.py', 'r', encoding='utf-8', errors='ignore') as f:
    content = f.read()

# Записываем обратно с правильной кодировкой
with codecs.open('lightweight_charts.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("Кодировка файла исправлена!") 