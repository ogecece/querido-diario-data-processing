# from .interfaces import DatabaseInterface, StorageInterface
from io import BytesIO
from database import create_database_interface
from storage import create_storage_interface
import xml.etree.cElementTree as ET
import hashlib, traceback
from datetime import datetime


def hash_text(text):
    """
    Receives a text and returns its SHA-256 hash of a text content
    """
    # Cria um objeto sha256
    hasher = hashlib.sha256()

    # Atualiza o objeto com o texto codificado em UTF-8
    hasher.update(text.encode('utf-8'))

    # Obtém o hash hexadecimal
    return hasher.hexdigest()

def txt_to_xml(path_xml, txt: str, meta_info: dict, storage):
    """
    Transform a .txt file into a .xml file and upload it to the storage bucket
    """
    # Cria uma tag (elemento) chamado 'root' e um subelemento deste chamado 'doc'
    root = ET.Element("root")
    meta_info_tag = ET.SubElement(root, "meta")

    # Cria um subelemento do 'doc' chamado 'field1' e 'field2' com atributos 'name' e um texto
    ET.SubElement(meta_info_tag, "data", name="dia").text = meta_info['dia']
    ET.SubElement(meta_info_tag, "data", name="mes").text = meta_info['mes']

    ET.SubElement(meta_info_tag, "localidade", name="municipio").text = "some vlaue2"
    ET.SubElement(meta_info_tag, "localidade", name="estado").text = "estado"
    ET.SubElement(meta_info_tag, "criado_em").text = "criado_em"

    gazettes_tag = ET.SubElement(root, "gazettes")
    
    ET.SubElement(gazettes_tag, "gazette").text = txt
    
    # Adiciona a uma árvore de elementos XML (ou seja, o elemento 'root' onde contém todo o documento)
    # e o adiciona a um arquivo binário que será enviado para o storage bucket em formato .xml
    tree = ET.ElementTree(root)

    file_xml = BytesIO()

    tree.write(file_xml, encoding='utf-8', xml_declaration=True)
    file_xml.seek(0) # Volta o cursor de leitura do arquivo para o começo dele

    content_file_xml = file_xml.getvalue().decode('utf-8')

    storage.upload_content(path_xml, content_file_xml)
    

def create_xml_for_territory_and_year(territory_info:tuple, database, storage):
    ano_atual = datetime.now().year
    ano = 1960

    while(ano <= ano_atual):

        query_content = database.select(f"SELECT * FROM gazettes\
                                WHERE territory_id='{territory_info[0]}' AND\
                                 date BETWEEN '{ano}-01-01' AND '{ano}-12-31'\
                                ORDER BY date ASC;")

        if len(list(query_content)) > 0:
            root = ET.Element("root")
            meta_info_tag = ET.SubElement(root, "meta")
            ET.SubElement(meta_info_tag, "localidade", name="municipio").text = territory_info[1]
            ET.SubElement(meta_info_tag, "localidade", name="estado").text = territory_info[2]
            ET.SubElement(meta_info_tag, "criado_em").text = str(datetime.now())
            ET.SubElement(meta_info_tag, "Ano").text = str(ano)
            all_gazettes_tag = ET.SubElement(root, "gazettes")  

            path_xml = f"{territory_info[0]}/{ano}/{territory_info[1]} - {territory_info[2]} - {ano}.xml"

            for gazette in query_content:
                
                arquivo = BytesIO()
                path_arq_bucket = str(gazette[7]).replace(".pdf",".txt") # É a posição 7 que contem o caminho do arquivo dentro do S3
                    
                try:
                    storage.get_file(path_arq_bucket, arquivo)
                except:
                    print(f"Não foi achado no {territory_info[0]} - {ano} - {gazette[2]}")
                    continue

                gazette_tag = ET.SubElement(all_gazettes_tag, "gazette")
                meta_gazette = ET.SubElement(gazette_tag, "meta")
                ET.SubElement(meta_gazette, "URL_PDF").text = gazette[8]
                ET.SubElement(meta_gazette, "Poder").text = gazette[5]
                ET.SubElement(meta_gazette, "Edicao_Extra").text = 'Sim' if gazette[4] else 'Não'
                ET.SubElement(meta_gazette, "Numero_Edicao").text = str(gazette[3])
                ET.SubElement(meta_gazette, "Data_Diario").text = datetime.strftime(gazette[2], "%d/%m")
                ET.SubElement(gazette_tag, "Conteudo").text = arquivo.getvalue().decode('utf-8')

                arquivo.close()
            
            tree = ET.ElementTree(root)

            file_xml = BytesIO()

            tree.write(file_xml, encoding='utf-8', xml_declaration=True)
            file_xml.seek(0) # Volta o cursor de leitura do arquivo para o começo dele

            content_file_xml = file_xml.getvalue().decode('utf-8')

            storage.upload_content(path_xml, content_file_xml)

            file_xml.close()
        else:
            print(f"Nada encontrado para cidade {territory_info[1]}-{territory_info[2]} no ano {ano}")

        ano += 1


def create_xml_territories():

    database = create_database_interface()
    storage = create_storage_interface()

    print("Script que agrega os arquivos .txt para .xml")

    # results_query = database.select("SELECT * FROM territories WHERE name='Sampaio' OR name='Xique-Xique';")
    results_query = database.select("SELECT * FROM territories;")

    for t in results_query:
        try:
            create_xml_for_territory_and_year(t, database, storage)
        except:
            print(traceback.format_exc())
            continue


if __name__ == "__main__":
    create_xml_territories()