#####################################################
## 내용 :                                           ##
##   스파크 환경 구축 1                                ##
##   Software-Development-Kit Manager(sdkman) 활용  ##
## version :                                       ##
##   spark  3.5.0                                  ##
##   scala  2.13.13                                ##
##   java   17.0.10                                ##
#####################################################


#install sdkman
curl -s "https://get.sdkman.io" | bash
# .zshrc / .bashrc / .bash_profile    
source "$HOME/.sdkman/bin/sdkman-init.sh"

# 참고
#sdk list spark
#sdk current 
#sdk use spark 3.5.0
#sdk default spark 3.5.0  #default version set 
#sdk upgrade spark    # version update

# install & set spark 3.5.0
sdk install spark 3.5.0  
sdk default spark 3.5.0
# install & set scala 2.13.13
sdk install scala 2.13.13
sdk default scala 2.13.13
# install & set java 17.0.10
sdk install java 17.0.10-oracle
sdk default java 17.0.10-oracle

#Using:                 
#java: 17.0.10-oracle
#scala: 2.13.13
#spark: 3.5.0
