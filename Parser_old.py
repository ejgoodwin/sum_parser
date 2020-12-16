#!/usr/bin/env python
# coding: utf-8

# In[332]:


from tkinter import *
from tkinter import ttk, filedialog
from tkinter.filedialog import askopenfilename
import csv
from pathlib import Path
import pymongo
# Use sentinel for missing values
from numpy import nan as NA
import pandas as pd

# connect to mongodb
client = pymongo.MongoClient("mongodb://localhost:27017")
# create db
db_formative = client["formative"]
# create collection
collection_vendors = db_formative["test-80"]


# In[333]:


# Global variables
fileButtons = ["inspections", "inventory", "violations"]
fileRoutes = {fileButtons[0]:"", fileButtons[1]:"", fileButtons[2]:""}


# In[334]:


class Parser(object):
    def __init__(self, collection, csvFile):
        self.collection = collection
        self.csvFile = csvFile
        self.dataList = []
        self.dataObj = {}
        
    def parseData(self):
        print(self.csvFile['inspections'])
#         with open(self.csvFile['inspections'], newline="", encoding='utf-8-sig') as inFile: 
#             dataReader = csv.DictReader(inFile)
        self.readInspections()
#         with open(self.csvFile['inventory'], newline="", encoding='utf-8-sig') as inFile: 
#             dataReader = csv.DictReader(inFile)
        self.readInventory()
#         with open(self.csvFile['violations'], newline="", encoding='utf-8-sig') as inFile: 
#             dataReader = csv.DictReader(inFile)
        self.readViolations()
                
    def readInspections(self):
        with open(self.csvFile['inspections'], newline="", encoding='utf-8-sig') as inFile: 
            dataReader = csv.DictReader(inFile)
            for row in dataReader:
                # Do not add inactive programs
                if row['PROGRAM STATUS'] == 'INACTIVE':
                    continue
                # Add Mongo ID as Serial Number with INS- prefix
                print(row['SERIAL NUMBER'])
                row['_id'] = "INS-"+row['SERIAL NUMBER']
                # Split PE Description and create new row with seating
                try:
                    start,mid = row["PE DESCRIPTION"].split(' (')
                    mid,end = mid.split(')')
                    row["PE DESCRIPTION"] = start+end
                    row["SEATING DETAILS"] = mid
                except:
                    print('no seating desc')
                # NEW: remove second half of zip (e.g. 90001-1234 remove '1234')
                try:
                    zipStart = row["FACILITY ZIP"].split('-')
                    row["FACILITY ZIP"] = zipStart[0]
                except:
                    print('no zip')
                self.dataList.append(row)
            print(f'Inspection first doc: \n{self.dataList[0]}')
            ins = self.collection.insert_many(self.dataList)
            print('=> Inspections inserted into DB')
            self.dataList = []

    def readInventory(self):
         with open(self.csvFile['inventory'], newline="", encoding='utf-8-sig') as inFile: 
            dataReader = csv.DictReader(inFile)
            for row in dataReader:
                # Add Mongo ID as Facility ID with F_ID- prefix
                print(row['FACILITY ID'])
                row['_id'] = 'F_ID-'+row['FACILITY ID']
                # Split PE Description and create new row with seating
                start,mid = row["PE DESCRIPTION"].split(' (')
                mid,end = mid.split(')')
                row["PE DESCRIPTION"] = start+end
                row["SEATING DETAILS"] = mid
                self.dataList.append(row)
            print(f'Inventory first doc: \n{self.dataList[0]}')
            ins = self.collection.insert_many(self.dataList)
            print('=> Inventory inserted into DB')
            self.dataList = []
            
    def readViolations(self):
        with open(self.csvFile['violations'], newline="", encoding='utf-8-sig') as inFile: 
            dataReader = csv.DictReader(inFile)
            # Group the rows by serial number so they can be added to the corresponding inspection
            for row in dataReader:
                print(row)
                if row['SERIAL NUMBER'] in self.dataObj:
                    self.dataObj[row['SERIAL NUMBER']].append(row)
                else:
                    self.dataObj[row['SERIAL NUMBER']] = [row]
            self.dataList.append(self.dataObj)
            print(f'Violations first doc: \n{self.dataList[0]}')
            ins = collection_vendors.insert_many(self.dataList)
            print('=> Violations inserted into DB')
            self.dataList = []
        
        
    def formatViolations(self):
        for violation in self.dataObj:
            collection_vendors.update_one({'_id': 'INS-'+violation}, {'$set': {'VIOLATIONS': self.dataObj[violation]}})


# In[335]:


class ButtonUpload(object):
    def __init__(self, fileButtons, fileLabels, counter, mainframe):
        self.fileButtons = fileButtons
        self.fileLabels = fileLabels
        self.counter = counter
        self.mainframe = mainframe
        self.filePath = ''
        self.uploadLabel = StringVar()

    def browseFile(self):
        self.filePath = askopenfilename(filetypes=[("CSV files", ".csv")])
        fileName = Path(self.filePath)
        self.uploadLabel.set(fileName.name)
        print(self.filePath)
        self.setFileRoutes()
    
    def setFileRoutes(self):
        fileRoutes[self.fileButtons] = self.filePath
        print(fileRoutes)
        
    def createButton(self):
        ttk.Label(self.mainframe, text=f"Upload data: {self.fileButtons}", anchor="w").grid(row=self.counter+1, column=0, sticky="w")
        Button(self.mainframe, text="Upload file", command=self.browseFile).grid(row=self.counter+1, column=1, sticky="w")
        ttk.Label(self.mainframe, textvariable=self.uploadLabel).grid(row=self.counter+1, column=2, sticky="w")  


# In[336]:


class AveragesCalculator(object):
    def __init__(self, year, category):
        self.year = year
        self.category = category
        
    def zipScores(self):
        # ******* TO DO: ERROR IF YEAR NOT FOUND ********
        print(f'Calculating stats for: \n- Year = {self.year} \n- Category = {self.category}')
        # Find inspection documents - look in ispections (INS-) with year
        resultsInspections = collection_vendors.find({'_id':{'$regex':'INS-'},'ACTIVITY DATE':{'$regex':self.year}})

        # Create dict of facility zips with lists of their scores
        zipScoresDict = {}  
        for r in resultsInspections:
            key = str(r[self.category])
            if(r['SCORE'] == ''):
                value = NA
            else:
                value = int(r['SCORE'])
            if key in zipScoresDict:
                zipScoresDict[r[self.category]].append(value)
            else:
                zipScoresDict[key] = [value]
        return self.calcZipStats(zipScoresDict)


    def calcZipStats(self, zipScoresDict):
        zipScoresMean = {}
        zipScoresMean = {self.category:[],'mean':[],'median':[],'mode':[]}
        for x in zipScoresDict:
            # Create Series to calc mean, mode, median
            tempSeries = pd.Series(zipScoresDict[x])
            mean = tempSeries.mean()
            mode = tempSeries.mode().tolist()
            median = tempSeries.median()

            # mean, mode, median as columns
            zipScoresMean[self.category].append(x)
            zipScoresMean['mean'].append(mean)
            zipScoresMean['median'].append(median)
            zipScoresMean['mode'].append(mode)

        # Create DataFrame
        zipScoresDF = pd.DataFrame(zipScoresMean)
        # sort by ascending cat - easier to find for user (esp. zip)
        zipScoresDF.sort_values(by=[self.category],inplace=True)
        # set cat as index
        zipScores_noIndex = zipScoresDF.set_index(self.category)
        
        return zipScores_noIndex
 


# In[337]:


class UserInterface(object):
    def __init__(self, title):
        self.title = title
        self.mainframe = None
        self.window = Tk()
        self.window.title(self.title)
        self.window.geometry("600x400")
        self.window.columnconfigure(0, weight=1)
        self.window.rowconfigure(0, weight=1)
        self.calcCategory = StringVar()
        self.calcYear = StringVar()
        
    def createWindow(self):
        self.createInitialFrame()
        self.window.mainloop()
        
    def createInitialFrame(self):
         # variables
        labelTextOne = StringVar()
        labelTextTwo = StringVar()
        labelTextThree = StringVar()
        fileLabels = [labelTextOne, labelTextTwo, labelTextThree]
        # Config layout
        self.mainframe = ttk.Frame(self.window, padding="16 16 16 16")
        self.mainframe.grid(column=0, row=0, sticky=(N, W, E, S))
        self.mainframe.grid_rowconfigure(4, weight=1)
        self.mainframe.grid_columnconfigure(2, weight=1)
        heading = ttk.Label(self.mainframe, text="Load your dataset", anchor="w").grid(row=0, sticky="nw")
        # Create buttons for file uploads
        for i in range(0, len(fileButtons)): 
            newButton = ButtonUpload(fileButtons[i], fileLabels[i], i, self.mainframe)
            newButton.createButton()
        # Buttons for Cancel and Clean & Save
        footer = ttk.Frame(self.window, padding="16 0 0 0").grid(row=5, column=1)
        cancelButton = Button(self.mainframe, text="Cancel & exit", bg="red").grid(row=4, column=0, sticky="sw")
        saveButton = Button(self.mainframe, text="Save", command=self.handleSave).grid(row=4, column=2,sticky="se")
        
    def handleSave(self):
        self.loadingFrame('Your data is being parsed and loaded into MongoDB...')
        parser = Parser(collection_vendors, fileRoutes)
        parser.parseData()
#         parser.formatViolations()
        self.createNotebook()
    
    def loadingFrame(self, text):
        self.mainframe.destroy()
        self.mainframe = ttk.Frame(self.window, padding="16 16 16 16")
        self.mainframe.grid(column=0, row=0, sticky=(N, W, E, S))
        heading = ttk.Label(self.mainframe, text="Please wait:", anchor="w").grid(row=0, sticky="nw")
        subheading = ttk.Label(self.mainframe, text=text, anchor="w").grid(row=1, sticky="nw")
        
    def createNotebook(self):
        print('here')
        self.mainframe.destroy()
        self.mainframe = ttk.Notebook(self.window)
        f1 = ttk.Frame(self.mainframe)  
        f2 = ttk.Frame(self.mainframe)  
        f3 = ttk.Frame(self.mainframe) 
        self.mainframe.add(f1, text='Averages')
        self.mainframe.add(f2, text='Violations per Establishment')
        self.mainframe.add(f3, text='Correlations')
        # ====== Widget for tab 1
        heading_one = ttk.Label(f1, text="Mean, Mode and Median", anchor="w").grid(row=0, sticky="nw")
        # Radio - Type
        radioHeadingOne = ttk.Label(f1, text="Calculate by seating type or zip codes:", anchor="w").grid(row=0, sticky="nw")
        Radiobutton(f1, text="Vendor seating type", padx = 20, variable=self.calcCategory, value='SEATING DETAILS').grid(row=1, column=0, sticky='w')
        Radiobutton(f1, text="Zip Code", padx = 20, variable=self.calcCategory, value='FACILITY ZIP').grid(row=2, column=0, sticky='w')
        # Radio - Years
        radioHeadingTwo = ttk.Label(f1, text="Select Year", anchor="w").grid(row=3, sticky="nw")
        yearsArr = ['2016', '2017', '2018', '2019']
        for i in range(len(yearsArr)):
            Radiobutton(f1, text=yearsArr[i], padx = 20, variable=self.calcYear, value=yearsArr[i]).grid(row=i+4, column=0, sticky='w')
        # Calc button
        calcButton = Button(f1, text="Calculate", command=self.calcAverages).grid(row=8, column=0, sticky="sw")
        self.mainframe.grid(column=0, row=0, sticky=(N, W, E, S))
        
        
        # ====== Widget for tab 2
        heading_two = ttk.Label(f2, text="Violations per Establishment", anchor="w").grid(row=0, sticky="nw")
    
    def calcAverages(self):
        calcCatVal = self.calcCategory.get()
        calcYearVal = self.calcYear.get()
        calcAv = AveragesCalculator(calcYearVal, calcCatVal)
        avResults = calcAv.zipScores()
        self.averagesResultsFrame(avResults, calcYearVal, calcCatVal)
        
    def averagesResultsFrame(self, resultsAv, calcYearVal, calcCatVal):
        avResultsWindow = Tk()
        avResultsWindow.title(f'Averages Results for {calcCatVal} in {calcYearVal}')
        avResultsWindow.geometry("600x400")
        avResultsWindow.columnconfigure(0, weight=1)
        avResultsWindow.rowconfigure(0, weight=1)
        mainframe = ttk.Frame(avResultsWindow, padding="16 16 16 16")
        mainframe.place(relheight=1, relwidth=1)

        tree = ttk.Treeview(mainframe)
        tree.place(relheight=1, relwidth=1)
        treescrolly = Scrollbar(mainframe, orient='vertical', command=tree.yview)
        treescrollx = Scrollbar(mainframe, orient='horizontal', command=tree.xview)
        tree.configure(xscrollcommand=treescrollx.set, yscrollcommand=treescrolly.set)
        treescrolly.pack(side='right', fill='y')
        treescrollx.pack(side='bottom', fill='x')

        tree["columns"]=("one","two","three")
        tree.column("#0", width=75, minwidth=75)
        tree.column("one", width=75, minwidth=75)
        tree.column("two", width=75, minwidth=75)
        tree.column("three", width=75, minwidth=75)

        tree.heading("#0",text="Seating")
        tree.heading("one", text="Mean")
        tree.heading("two", text="Median")
        tree.heading("three", text="Mode")

#         data = resultsAv.to_numpy()

        for index, row in resultsAv.iterrows(): 
            tree.insert('','end', text=index, values=[row['mean'],row['median'],row['mode']])
        print(f'This is resultsAv: \n{resultsAv}')
 


# In[338]:


ui = UserInterface('summative')
ui.createWindow()


# In[190]:


avResultsWindow = Tk()
avResultsWindow.title('Averages Results')
avResultsWindow.geometry("600x400")
avResultsWindow.columnconfigure(0, weight=1)
avResultsWindow.rowconfigure(0, weight=1)
mainframe = ttk.Frame(avResultsWindow, padding="16 16 16 16")
mainframe.place(relheight=1, relwidth=1)

tree = ttk.Treeview(mainframe)
tree.place(relheight=1, relwidth=1)
treescrolly = Scrollbar(mainframe, orient='vertical', command=tree.yview)
treescrollx = Scrollbar(mainframe, orient='horizontal', command=tree.xview)
tree.configure(xscrollcommand=treescrollx.set, yscrollcommand=treescrolly.set)
treescrolly.pack(side='right', fill='y')
treescrollx.pack(side='bottom', fill='x')

# headings = ['1','2','3']
data = [['mode',1,2,3], ['mode',1,2,3], ['mode',1,2,3]]

# for h in headings:
#     tree.heading(h, text=h)
tree["columns"]=("one","two","three")
# tree.column("#0", width=270, minwidth=270)
tree.column("one", width=150, minwidth=150)
tree.column("two", width=400, minwidth=200)
tree.column("three", width=80, minwidth=50)

# tree.heading("#0",text="Name")
tree.heading("one", text="Date modified")
tree.heading("two", text="Type")
tree.heading("three", text="Size")
for d in data:
    tree.insert('','end', text=d[0], values=d[1:])
#     tree.insert('', 'end', text=d)
#     print(d)
# tree.heading(column, text='1')
# tree.insert('', 'end', text='Tutorial')
# tree.insert('','end',values='1')

# Level 1
# folder1=tree.insert("", 1, "", text="Folder 1", values=("23-Jun-17 11:05","File folder",""))
# tree.insert("", 2, "", text="text_file.txt", values=("23-Jun-17 11:25","TXT file","1 KB"))
# Level 2
# tree.insert('', "end", text="photo1.png", values=("22-Jun-17 11:28","PNG file","2.6 KB"))
# tree.insert('', "end", text="photo2.png", values=("24-Jun-17 11:29","o file","3.2 KB"))
# tree.insert('', "end", text="photo3.png", values=("25-Jun-17 11:30","p file","3.1 KB"))
avResultsWindow.mainloop()


# In[192]:


calcAv = AveragesCalculator('2018', 'SEATING DETAILS')
df = calcAv.zipScores()

headings = df.columns

for h in headings:
    print(h)


# In[211]:


df


# In[224]:


nparr = df.to_numpy()
nparrIndex = df.index.to_numpy()
nparr
nparrIndex


# In[216]:


npa[0]


# In[237]:


for index, row in df.iterrows(): 
    print(index, row['mean'], row['mode'])
    break


# In[265]:


avResultsWindow = Tk()
avResultsWindow.title('Averages Results')
avResultsWindow.geometry("600x400")
avResultsWindow.columnconfigure(0, weight=1)
avResultsWindow.rowconfigure(0, weight=1)
mainframe = ttk.Frame(avResultsWindow, padding="16 16 16 16")
mainframe.place(relheight=1, relwidth=1)

tree = ttk.Treeview(mainframe)
tree.place(relheight=1, relwidth=1)
treescrolly = Scrollbar(mainframe, orient='vertical', command=tree.yview)
treescrollx = Scrollbar(mainframe, orient='horizontal', command=tree.xview)
tree.configure(xscrollcommand=treescrollx.set, yscrollcommand=treescrolly.set)
treescrolly.pack(side='right', fill='y')
treescrollx.pack(side='bottom', fill='x')

tree["columns"]=("one","two","three")
tree.column("#0", width=75, minwidth=75)
tree.column("one", width=75, minwidth=75)
tree.column("two", width=75, minwidth=75)
tree.column("three", width=75, minwidth=75)

tree.heading("#0",text="Seating")
tree.heading("one", text="Mean")
tree.heading("two", text="Median")
tree.heading("three", text="Mode")

data = df.to_numpy()

for index, row in df.iterrows(): 
    tree.insert('','end', text=index, values=[row['mean'],row['median'],row['mode']])
    
avResultsWindow.mainloop()


# In[ ]:




