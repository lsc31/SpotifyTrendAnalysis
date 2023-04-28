import pandas as pd
from sklearn.metrics import confusion_matrix, classification_report
from sklearn.model_selection import train_test_split
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from textblob import TextBlob
import matplotlib.pyplot as plt
import numpy as np
import itertools
import re

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.pipeline import make_pipeline
from sklearn.svm import LinearSVC
import pickle


def plot_confusion_matrix(cm, classes, accuracy, title='Confusion matrix', cmap=plt.cm.Blues):
    """
    This function prints and plots the confusion matrix.
    """
    plt.imshow(cm, interpolation='nearest', cmap=cmap)
    plt.title(f"{title}\nAccuracy: {accuracy:.2f}", fontsize=20)
    plt.colorbar()
    tick_marks = np.arange(len(classes))
    plt.xticks(tick_marks, classes, rotation=45, fontsize=16)
    plt.yticks(tick_marks, classes, fontsize=16)
    
    # Add labels to each cell
    thresh = cm.max() / 2.
    for i, j in itertools.product(range(cm.shape[0]), range(cm.shape[1])):
        plt.text(j, i, format(cm[i, j], 'd'), horizontalalignment="center", fontsize=18, color="white" if cm[i, j] > thresh else "black")
    
    plt.tight_layout()
    plt.ylabel('True label', fontsize=18)
    plt.xlabel('Predicted label', fontsize=18)
    
    plt.show()

def cleanTweet(tweet: str) -> str:
    tweet = re.sub(r'http\S+', '', str(tweet))
    tweet = re.sub(r'bit.ly/\S+', '', str(tweet))
    tweet = tweet.strip('[link]')

    # remove users
    tweet = re.sub('(RT\s@[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))
    tweet = re.sub('(@[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))

    # remove puntuation
    my_punctuation = '!"$%&\'()*+,-./:;<=>?[\\]^_`{|}~•@â'
    tweet = re.sub('[' + my_punctuation + ']+', ' ', str(tweet))

    # remove number
    # tweet = re.sub('([0-9]+)', '', str(tweet))

    # remove hashtag
    tweet = re.sub('(#[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))

    return tweet
    

# Load the dataset
data = pd.read_csv('twitter_validation2.csv')

# Preprocess the tweets
data['Tweet'] = data['Tweet'].apply(cleanTweet)

# Split the data into training and testing sets
X_test = data['Tweet']
y_test = data['Sentiment']
labels = np.array(['Positive', 'Negative', 'Neutral'])

# Initialize the VADER sentiment analyzer and TextBlob sentiment analyzer
analyzer_vader = SentimentIntensityAnalyzer()
analyzer_textblob = TextBlob

# Get the predicted sentiment for each text in the test set using VADER
y_pred_vader = [analyzer_vader.polarity_scores(text)['compound'] for text in X_test]
y_pred_vader = ['Positive' if score >= 0.05 else 'Negative' if score <= -0.05 else 'Neutral' for score in y_pred_vader]

# Get the predicted sentiment for each text in the test set using TextBlob
y_pred_textblob = [analyzer_textblob(text).sentiment.polarity for text in X_test]
y_pred_textblob = ['Positive' if score > 0 else 'Negative' if score < 0 else 'Neutral' for score in y_pred_textblob]

# Create the confusion matrix and classification report for VADER
conf_matrix_vader = confusion_matrix(y_true=y_test, y_pred=y_pred_vader, labels=labels)
class_report_vader = classification_report(y_test, y_pred_vader)

# Create the confusion matrix and classification report for TextBlob
conf_matrix_textblob = confusion_matrix(y_true=y_test, y_pred=y_pred_textblob, labels=labels)
class_report_textblob = classification_report(y_test, y_pred_textblob)

# Sentiment analysis using SVM
# Load the dataset
data2 = pd.read_csv('twitter_training.csv')

# Split the data into training and testing sets
# X_train, X_test, y_train, y_test = train_test_split(data2['Tweet'], data2['Sentiment'], test_size=0.2, random_state=1)
X_train = data2['Tweet'].apply(cleanTweet)
y_train = data2['Sentiment']

# Create a pipeline for SVM
pipeline = make_pipeline(TfidfVectorizer(), LinearSVC())

# Fit the pipeline
pipeline.fit(X_train, y_train)

# save model to file
filename = 'svm_model.pickle'
with open(filename, 'wb') as file:
    pickle.dump(pipeline, file)

# Get the predictions
y_pred_svm = pipeline.predict(X_test)

# Create the confusion matrix and classification report for SVM
conf_matrix_svm = confusion_matrix(y_true=y_test, y_pred=y_pred_svm, labels=labels)
class_report_svm = classification_report(y_test, y_pred_svm)

# Print the results
print('Confusion Matrix for VADER:')
print(conf_matrix_vader)
print('\nClassification Report for VADER:')
print(class_report_vader)

print('Confusion Matrix for TextBlob:')
print(conf_matrix_textblob)
print('\nClassification Report for TextBlob:')
print(class_report_textblob)

print('Confusion Matrix for SVM:')
print(conf_matrix_svm)
print('\nClassification Report for SVM:')
print(class_report_svm)

vader_accuracy = (conf_matrix_vader[0,0]+conf_matrix_vader[1,1]+conf_matrix_vader[2,2])/conf_matrix_vader.sum()
textblob_accuracy = (conf_matrix_textblob[0,0]+conf_matrix_textblob[1,1]+conf_matrix_textblob[2,2])/conf_matrix_textblob.sum()
svm_accuracy = (conf_matrix_svm[0,0]+conf_matrix_svm[1,1]+conf_matrix_svm[2,2])/conf_matrix_svm.sum()

plot_confusion_matrix(conf_matrix_vader, classes=labels, accuracy=vader_accuracy, title="VADER Confusion matrix")
plot_confusion_matrix(conf_matrix_textblob, classes=labels, accuracy=textblob_accuracy, title="TextBlob Confusion matrix")
plot_confusion_matrix(conf_matrix_svm, classes=labels, accuracy=svm_accuracy, title="SVM Confusion matrix")
