//daemon est constituer d'une classe englobant toute les méthodes
class AcquirerBankaccount extends Backend_Pgq_Consumer
{


//La méthode processEvent() va déroulet le processus du daemon et appeller toute les méthodes nécessaires aux étapes
public function processEvent(Himedia_Pgq_Event $event){

        1/ annonce dans le logger le début de process_event
        2/ Récupère et check les infos de $event
            
            $datalog = $event->getData();
        //Définit des vaiable pour la méthode preg_replace qui sert à cibler puis remplacer du contenu dans un string
            $pattern = '/\s(\d{6})(\d{6})(\d{4})\s/';
            $replacement = ' $1******$3 ';

            Si $datalog contient une colonne 'description'
                => remplacer sont contenu avec la méthode preg_replace
                    $datalog['description'] = preg_replace($pattern, $replacement, $datalog['description']);
            Si $datalog contient une colonne 'short_memo'
                => remplacer sont contenu avec la méthode preg_replace
                    $datalog['short_memo'] = preg_replace($pattern, $replacement, $datalog['short_memo']);
        
        3/ Interroge le backend et le stocke dans une variable $adapter
                $adapter = $this->getDb('backend');
//Debut TRY
        1/ Crée diverses variable permettant de stocker la réponse de requête
            => Stock dans $data le résultat de la méthode detData()
            => Stock dans $mapper le résultat de la méthode AcquirerReconciliation_Bankaccount($this->getDb('backend'), $data, $this->_logger)
            => Récupère ensuite le acctid de $mapper avec la méthode findBankAccountidByIban($data['iban']) dans la variable $acctid

        2/ Check si $acctid est vide 
                Si il est vide => msg de l'erreur dans le logger
                Si non 
                    => Ecrase une nouvelle fois la variable $mapper avec la méthode setBankAccountid($acctid) 
                    => Check si le compte est déja dans la la DB avec la méthode alreadyInFinance()
                        Si oui => envoi un msg 'tranfert already réconcilied' au logger
                        Si non 
                            if avec la méthode insertInFinance() //$mapper->insertInFinance()
                                si oui => msg au logger 'Financial transaction insert in finance'
                                si non => msg au logger 'Financial transaction already exist'
                            if avec la méthode findIfTrxTypeMustBeReconciled() //(!$mapper->findIfTrxTypeMustBeReconciled($data['trxtype']))
                                si oui => msg au logger 'Bank transacrion type ${azdazd} must not be réconcilied'
                                si non => lance la méthode insertInFundingReconciliationQueue()


//FIN TRY

    }


}