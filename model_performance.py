##############################################################################
#-----------------------------------------------------------------------------
#                            Program Information
#-----------------------------------------------------------------------------
# Author                 : Amber Zaratisan
# Creation Date          : 12APR2018
#-----------------------------------------------------------------------------
#-----------------------------------------------------------------------------
#                             Script Information
#-----------------------------------------------------------------------------
# Script                 : model_performance.py
# Bitbucket Repo         :
# Brief Description      : This script will print the AUC and Gini score and
#                          plot an ROC curve for a given model. 
# Data used              : 
# Output Files           : 
#
# Notes / Assumptions    : The function requires three arguments:
#                              1) input model name
#                              2) name of test sample
#                              3) name of test true labels
#                          This function requires sklearn and matplotlib to be 
#                          installed.
#-----------------------------------------------------------------------------
#                            Environment Information
#-----------------------------------------------------------------------------
# Python Version         : 3.6.2
# Anaconda Version       : 5.0.1
# Spark Version          : n/a
# Operating System       : Windows 10
#-----------------------------------------------------------------------------
#                           Change Control Information
#-----------------------------------------------------------------------------
# Programmer Name/Date   : Change and Reason
#
##############################################################################


def roc_curve(model, X_test, y_test):
    """
    import model_performance
    model_performance.roc_curve(model=lr, X_test=X_test, y_test=y_test)
    """

    try:
        from sklearn import metrics
    except:
        print('Sklearn package not installed. Required for this function')
        raise

    try:
        import matplotlib.pyplot as plt
    except:
        print('Matplotlib package not installed. Required for this function')
        raise

    probas = model.predict_proba(X_test)  # Probability estimates

    predictions = probas[:, 1]

    # Calculate the fpr and tpr for all thresholds of the classification
    fpr, tpr, threshold = metrics.roc_curve(y_test, predictions)
    auc = metrics.auc(fpr, tpr)

    # Prints the AUC Score
    print('AUC Score:')
    print(format(auc, '.3f'))

    # Prints the Gini Score
    print('\nGini Score:')
    print(format((2*auc-1), '.3f'))

    # Plot ROC curve
    plt.title('ROC Curve')
    plt.plot(fpr, tpr, 'b', label='Target')
    plt.legend(loc='lower right')
    plt.plot([0, 1], [0, 1], 'r--')
    plt.xlim([0, 1])
    plt.ylim([0, 1])
    plt.ylabel('True Positive')
    plt.xlabel('False Positive')
    plt.show()
