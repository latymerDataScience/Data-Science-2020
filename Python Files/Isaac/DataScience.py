###############################################################################################################
###############################################################################################################
###############################################################################################################
#                                               README                                                        #
###############################################################################################################
###############################################################################################################
###############################################################################################################


#GENERAL
#
#Current length: 374 lines
#
#This code is set out with comments throughout to describe what is happening through the program. 
#I have used various other modules - they all come with the standard python interpretter, so no install is required
#If you have any questions, feel free to raise an issue
#
#
#
#FUTURE DEVELOPMENT
#
#As this is still under development, some features that will be added have been detailed.
#The main extra development will be the addition of a basic user interface through the console.
#This will allow you to run all the functions that I will write to solve the various tasks.
#I have used an object oriented approach - the data sets are imported and then processed by the initialisation function.
#Future data can be queried without re-analysis.
#The data will eventually be picked (serialised) into a file, so that processed data can be reopened without having to
#Reprocess.
#
#
#
#Hope you enjoy the code :)



#Modules
import pandas as pd #Pandas Module
import re #Regular Expressions Module
import time #Time modile
import sys #System module

#CONSTANT DEFINITIONS
NOMEANCALC = "FLAG|0001"
ALLINDEX = "FLAG|0002"
DONTCHANGE = "FLAG|0003"


#A pretty print mechanism to output the data
def dotPrint(mainItem, #The heading
            value, #The Value
            dotLength #Number of dots to print inbetween
            ):

    text = mainItem
    for i in range (0, dotLength - len(mainItem)):
        text += "."
    text += "  " + value
    return text

#A Dataset
class DataSet:

    #Constructor function
    def __init__(self,
                filepath, #The file path to the data sources
                extraFactors, #The other factors in the data sources (assuming file name for the different factor)
                years, #The years the data was taken (assuming file name from diiferent year)
                NOINSIG = False, #Whether to keep insignificant values
                displayUncalculableMean = False #Whether to output if the mean of an attribute can't be displayed
                ):
        #Importing the constructor variables
        self.filepath = filepath
        self.extraDataSections = extraFactors
        self.years = years
        self.NOINSIG = NOINSIG
        self.displayUncalculableMean = displayUncalculableMean
        
        #Initialising the structures
        self.dataList = {} #Stores the Pandas Datasets in the format {year : {extraDataSection : Dataset} }
        self.attributes = {} #Stores the attributes in the format {extraDataSection: [attributes] }
        self.means = {} #Stores the mean values for all the sets in the format {year : {extraDataSection : {attribute : mean} } }
        self.medians = {} #Stores the median values in the same format as the means
        self.meanDifferences = {} #Stores the differences in the means over time in the format {extraDataSection : {attribute : mean} }
        
        #Populator functions
        self.getData()
        self.cleanAllData()
        self.getMeans()
        self.getMedians()
        self.getMeanDifferences()


    #Imports the data from a .csv (comma separated value) file
    def getData(self):
        #Create an empty dictionary to hold the information
        for year in self.years:
            self.dataList[year] = {}
            for extraDataSection in self.extraDataSections:
                self.attributes[extraDataSection] = []
                #Add the data to its extraDataSection in the dictionary
                self.dataList[year][extraDataSection] = pd.read_csv(self.filepath + extraDataSection + "-" + year + ".csv")
                attribList = list(self.dataList[year][extraDataSection])
                #Add the attributes to the attributes list
                for attribute in attribList:
                    if not (attribute in self.attributes):
                        if(attribute[0:8] != "Unnamed:"):
                            self.attributes[extraDataSection].append(attribute)
    
                            

#######################################AVERAGE GETTING FUNCTIONS###################################
                            


    #Gets the mean values for each attribute in each Dataset in the datalist
    def getMeans(self):
        for year in self.years:
            self.means[year] = {}
            for extraDataSection in self.extraDataSections:
                self.means[year][extraDataSection] = {}
                for attrib in self.attributes[extraDataSection]:
                    #If the mean can't be calculated, the NOMEANCALC flag is set
                    try:
                        self.means[year][extraDataSection][attrib] = self.dataList[year][extraDataSection][attrib].mean(axis = 0)
                    except:
                        self.means[year][extraDataSection][attrib] = NOMEANCALC

    #Gets the mean values for each attribute in each Dataset in the datalist
    def getMedians(self):
        for year in self.years:
            self.medians[year] = {}
            for extraDataSection in self.extraDataSections:
                self.medians[year][extraDataSection] = {}
                for attrib in self.attributes[extraDataSection]:
                    #If the mean can't be calculated, the NOMEANCALC flag is set
                    try:
                        self.medians[year][extraDataSection][attrib] = self.dataList[year][extraDataSection][attrib].median(axis = 0)
                    except:
                        pass

    
    #Gets the differencnces in the means between the sets
    def getMeanDifferences(self):
        totalCurrentMean = {} #The current running total
        numberAdded = {} #The number of places added to the total
        for attribute in self.attributes[self.extraDataSections[0]]:
            totalCurrentMean[attribute] = 0
            numberAdded[attribute] = 0
        for exDatSec in self.extraDataSections:
            self.meanDifferences[exDatSec] = {}
            for attribute in self.attributes[exDatSec]:
                self.meanDifferences[exDatSec][attribute] = {}
                try:
                    #Calculate the difference
                    self.meanDifferences[exDatSec][attribute] = self.means[self.years[0]][exDatSec][attribute] - self.means[self.years[len(self.years) -1]][exDatSec][attribute]
                except:
                    self.meanDifferences[exDatSec][attribute] = NOMEANCALC
                try:
                    totalCurrentMean[attribute] += self.means[self.years[0]][exDatSec][attribute] - self.means[self.years[len(self.years) -1]][exDatSec][attribute]
                    numberAdded[attribute] += 1
                except:
                    pass

        self.meanDifferences[ALLINDEX] = {}
        for attribute in self.attributes[self.extraDataSections[0]]:
            #if the total isn't zero - otherwise we would hit a divide by 0 error
            if totalCurrentMean[attribute] != 0:
                #Find the mean difference (total/number of items)
                self.meanDifferences[ALLINDEX][attribute] = totalCurrentMean[attribute] / numberAdded[attribute]
            else:
                self.meanDifferences[ALLINDEX][attribute] = NOMEANCALC


##########################PRINTING FUNCTIONS#############################


    #Gets the differences between the means of all attributes of 2 data sets. 
    #A positive difference shows an increase, negative shows a decrease from set one to two
    def printMeanDifference(self, exDatSec, #The extra data section to investigate
                          attrib, #The attribute to investigate
                          NOINSIG = DONTCHANGE  #Controls the significance remover
                          ):
            if(NOINSIG == DONTCHANGE):
                NOINSIG = self.NOINSIG
            meanDifference = self.meanDifferences[exDatSec][attrib]
            if meanDifference == NOMEANCALC:
                if self.displayUncalculableMean:
                    print(dotPrint(attrib, "Mean not available", 40))
            else: 
                if not NOINSIG:
                    #Check if the mean is within the significance range
                    if self.isSignificant(meanDifference, self.means[self.years[0]][exDatSec][attrib], 3):
                        #Prints the difference using the pretty print dot function
                        print(dotPrint(attrib, str(meanDifference), 40))
                else:
                    print(dotPrint(attrib, str(meanDifference), 40))
                
        

    
    #Prints the means of the data list
    ###DEVELOPING NOTE - This function should be updated to be able to print any of the averages
    def printMeans(self):
        for year in self.years:
            print("\n=======================================================\n" "                            "
                  + year.upper() + "                            " + "\n=======================================================\n")
            for extraDataSection in self.extraDataSections:
                print("\n========================\n" + extraDataSection.upper() + "\n========================\n")
                for attribute in self.attributes[extraDataSection]:
                    if self.means[year][extraDataSection][attribute] == NOMEANCALC:
                        if self.displayUncalculableMean:
                                print(dotPrint(attribute, "Mean not available", 40))
                        
                    else:
                        print(dotPrint(attribute, str(self.means[year][extraDataSection][attribute]), 40))

                        
               


    #Activates the printing of the differences of all of the sets
    def printAllDifferences(self, dataSetsSameParameters = True #If the data set attributes are the same
                            ):
        #For each extraDataSection
        print("MEAN DIFFERENCES\n")
        for extraDataSection in self.extraDataSections:
            print("\n========================\n" + extraDataSection.upper() + "\n========================\n")
            for attribute in self.attributes[extraDataSection]:
                #Pretty print the extraDataSection heading 
                #Print the differences in the means
                self.printMeanDifference(extraDataSection, attribute)
                #DEVELOPER NOTE - A median difference or range difference function should be added here
                #with the same syntax: self.printMedianDifference(extraDataSection, attribute)

        #Then do the same as above for all of the extra data sections combined 
        if(dataSetsSameParameters):
            print("\n========================\n" + "ALL" + "\n========================\n")
            for attribute in self.attributes[self.extraDataSections[0]]:
                self.printMeanDifference(ALLINDEX, attribute, True)
       

###################################CLEAN UP FUNCTIONS#############################
         
    #Cleans up a single data set
    def cleanData(self, dataSet, #The set to clean
                  currentDatSec #The current section of the data
                  ):
                
        #For all the columns in the data set
        for i in range(0,len(dataSet) ,1):
            #Catch errors where the program tries to convert a string to a float
            try:
                #Replace tr with 0.025
                dataSet.iloc[i] = dataSet.iloc[i].str.replace({"tr": 0.025})
            except:
                pass
                
            for j in range(0, len(dataSet.columns), 1):
                try:
                    #Replace non alpha-numeric characters with nothing (Using regular expressions)
                    dataSet.iloc[i, j] = re.sub(r'\W+', '', dataSet.iloc[i, j])  
                except:
                    pass
                
           
        for attribute in self.attributes[currentDatSec]:
            try:
                #Convert all the values to float
                dataSet[attribute] = dataSet[attribute].astype("float")
                sys.stdout.write("Converting " + attribute + " Succeeded\n" )
            except:
                sys.stdout.write("Converting " + attribute + " Failed\n" )
                pass
        return dataSet



    #Cleans all the data in multiple data sets
    def cleanAllData(self):
        for year in self.years:
            for extraDataSection in self.extraDataSections:
                self.dataList[year][extraDataSection] = self.cleanData(self.dataList[year][extraDataSection], extraDataSection)
                
       

########################CHECKING FUNCTIONS######################
#Checks if a value is inside a percentage either side of another value
    def isSignificant(self, difference, #The value to determine the significance of
                     startVal, #The value to compare with
                    percentageforsig #The significance percentage
                    ):  
        if abs(difference/startVal) *100 > percentageforsig:
            return True
        else:
            return False

    









#################################################################################
###################################TASKS#########################################
#################################################################################


#Task 1 - Cleaning and preprocessing

def doTask1():
    #Path to the data folder
    filepath = "input/ldsedexcel/"

    #The places in the data
    places = ["beijing", "camborne", "heathrow", "hurn", "jacksonville", "leeming", "leuchars", "perth"]

    #The years for the data
    years = ["1987", "2015"]

    #Create a new data object
    data = DataSet(filepath, places, years)
    #Print the differences
    data.printAllDifferences()


########################################################################################################################
#####################################  ###  ###   ###  ###    ######  ___#####      ####################################
#####################################  ###  ###  #  #  ### ###  ####  ___#####  ########################################
######################################     ####  ##    ###    ######     #####  ########################################
########################################################################################################################
#######    #####  ___####  ####  ###  ___####  #######    ####     ####  ###  ###  ___###   ###  #####      ############
####### ###  ###  ___#####  ##  ####  ___####  #####  ###  ###  ^^^####  # #  ###  ___###  #  #  #######  ##############
#######    #####     #######  ######     ####    #####    ####  #######  ###  ###     ###  ##    #######  ##############
########################################################################################################################


def doTask2():
    #Path to the data folder
    filepath = "input/ocrlds/OCR-lds-"

    #The places in the data
    categories = ["age", "travel"]

    #The years for the data
    years = ["2001", "2011"]

    #Create a new data object
    data = DataSet(filepath, categories, years, displayUncalculableMean = True)

    #data.printMeans()



#########################################################################################################################
#########################################################################################################################
#########################################################################################################################
#########################################################################################################################
#########################################################################################################################
#########################################################################################################################
#########################################################################################################################



##########################################################################################
#                               MAIN PROGRAM STARTS HERE                                 #
##########################################################################################


#UNCOMMENT THE FUNCTION YOU WANT TO RUN
doTask1()