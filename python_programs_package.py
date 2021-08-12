#!/usr/bin/env python
# coding: utf-8

# In[2]:


import os

f=os.popen("pip install jieba")
print('jieba', f.read())
f=os.popen("pip install beautifulsoup4")
print('beautifulsoup4', f.read())
f=os.popen("pip install pandas")
print('pandas', f.read())
f=os.popen("pip install keras")
print('keras', f.read())
f=os.popen("pip install flask")
print('flask', f.read())
f=os.popen("pip install matplotlib")
print('matplotlib', f.read())
f=os.popen("pip install numpy")
print('numpy', f.read())
f=os.popen("pip install pymongo")
print('pymongo', f.read())
f=os.popen("pip install pymysql")
print('pymysql', f.read())
f=os.popen("pip install requests")
print('requests', f.read())
f=os.popen("pip install scikit-learn")
print('scikit-learn', f.read())
f=os.popen("pip install tensorflow")
print('tensorflow', f.read())
f=os.popen("pip install wordcloud")
print('wordcloud', f.read())


# In[ ]:




