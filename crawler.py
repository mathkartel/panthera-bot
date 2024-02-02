import asyncio
import aiohttp
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
from datetime import datetime
import re
import json
import sys

async def fetch_page(url, session):
    try:
        headers = {
            'User-Agent': 'aparda-bot',
            'Referer': 'https://aparda.com/'
        }
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                # Tentar várias codificações até encontrar uma que funcione
                possible_encodings = ['utf-8', 'iso-8859-1', 'windows-1252']
                for encoding in possible_encodings:
                    try:
                        return await response.text(encoding=encoding)
                    except Exception as e:
                        print(f"Error decoding with {encoding}: {e}")
                # Se nenhuma codificação funcionar, retorne None
                print("Failed to decode page with any encoding")
                return None
            else:
                return None
    except Exception as e:
        print("Error fetching page:", e)
        return None

async def get_page(url, session):
    return await fetch_page(url, session)

async def extract_metadata(soup):
    metadata = {}
    metadata['title'] = soup.title.string.strip() if soup.title else ""

    try:
        metadata['description'] = soup.find('meta', attrs={'name': 'description'})['content'][:165]
    except (TypeError, KeyError):
        metadata['description'] = ""

    try:
        metadata['keywords'] = soup.find('meta', attrs={'name': 'keywords'})['content']
    except (TypeError, KeyError):
        metadata['keywords'] = ""

    try:
        metadata['favicon'] = soup.find('link', attrs={'rel': 'icon'})['href']
    except (TypeError, KeyError):
        metadata['favicon'] = ""

    # Adicionar campos do OpenGraph
    og_tags = soup.find_all('meta', attrs={'property': re.compile(r'^og:')})
    og_metadata = {}
    for tag in og_tags:
        property_name = tag['property'][3:]  # Remover o prefixo "og:"
        og_metadata[property_name] = tag['content']
    metadata.update(og_metadata)

    return metadata

async def extract_headings(soup):
    headings = {}
    headings['h1'] = soup.find('h1').text.strip() if soup.find('h1') else ""
    headings['h2'] = ", ".join([h.text.strip() for h in soup.find_all('h2')]) if soup.find_all('h2') else ""
    headings['h3'] = ", ".join([h.text.strip() for h in soup.find_all('h3')]) if soup.find_all('h3') else ""
    headings['h4'] = ", ".join([h.text.strip() for h in soup.find_all('h4')]) if soup.find_all('h4') else ""
    headings['h5'] = ", ".join([h.text.strip() for h in soup.find_all('h5')]) if soup.find_all('h5') else ""
    headings['h6'] = ", ".join([h.text.strip() for h in soup.find_all('h6')]) if soup.find_all('h6') else ""
    return headings

async def extract_links(soup, current_url):
    internal_links = set()
    external_links = []

    current_domain = urlparse(current_url).netloc

    for link in soup.find_all('a', href=True):
        href = link['href']
        if href.startswith('tel:') or href.startswith('mailto:') or href.startswith('#'):
            continue
        if not href.startswith('http'):
            href = urljoin(current_url, href)
        parsed_href = urlparse(href)
        if parsed_href.scheme in ['http', 'https']:
            if parsed_href.netloc == current_domain:
                internal_links.add(href)
            else:
                external_links.append(href)

    internal_links = list(internal_links)
    external_links = [{"url": link} for link in external_links]

    return internal_links, external_links

async def extract_body(soup):
    for script in soup(["script", "style"]):
        script.extract()
    text = soup.get_text(separator=" ")
    
    # Remover caracteres de quebra de linha e espaços em branco extras
    text = re.sub(r'\s+', ' ', text)
    
    # Remover espaços extras no início e no final
    text = text.strip()
    
    return text

async def get_favicon(soup, current_url):
    favicon_tag = soup.find('link', attrs={'rel': 'icon'})
    favicon = favicon_tag['href'] if favicon_tag else ""

    # Verificar se o favicon está faltando o domínio ou é relativo
    if favicon:
        if not favicon.startswith(('http://', 'https://')):
            # Se for relativo, torná-lo absoluto
            favicon = urljoin(current_url, favicon)
        elif favicon.startswith("./") or favicon.startswith("/"):
            # Se começar com "./" ou "/", também torná-lo absoluto
            parsed_current_url = urlparse(current_url)
            favicon = parsed_current_url.scheme + "://" + parsed_current_url.netloc + favicon

    return favicon

async def extract_sitemap_urls(sitemap_url):
    sitemap_urls = []
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(sitemap_url) as response:
                if response.status == 200:
                    soup = BeautifulSoup(await response.text(), 'lxml')
                    loc_tags = soup.find_all('loc')
                    sitemap_urls.extend([loc.text.strip() for loc in loc_tags])
    except Exception as e:
        print("Error extracting URLs from sitemap:", e)
    return sitemap_urls

async def check_sitemap(url):
    sitemap_url = urljoin(url, "/sitemap.xml")
    sitemap_index_url = urljoin(url, "/sitemap_index.xml")
    sitemap_urls = []

    try:
        async with aiohttp.ClientSession() as session:
            response = await session.head(sitemap_url)
            if response.status == 200:
                sitemap_urls.extend(await extract_sitemap_urls(sitemap_url))

            response = await session.head(sitemap_index_url)
            if response.status == 200:
                async with session.get(sitemap_index_url) as resp:
                    soup = BeautifulSoup(await resp.text(), 'lxml')
                    sitemap_tags = soup.find_all('sitemap')
                    for sitemap in sitemap_tags:
                        loc_tag = sitemap.find('loc')
                        if loc_tag:
                            sitemap_urls.extend(await extract_sitemap_urls(loc_tag.text.strip()))
    except Exception as e:
        print("Error checking sitemap:", e)

    return sitemap_urls

async def extract_schema(soup):
    schemas = []

    # Encontrar todas as tags <script> com o tipo de conteúdo 'application/ld+json'
    script_tags = soup.find_all('script', type='application/ld+json')

    # Iterar sobre todas as tags <script>
    for script_tag in script_tags:
        try:
            # Analisar o conteúdo JSON dentro da tag <script>
            schema_data = json.loads(script_tag.string)

            # Verificar se o conteúdo JSON contém o campo '@type' (tipo do esquema)
            if '@type' in schema_data:
                schema = {}

                # Extrair o tipo do esquema
                schema['@type'] = schema_data['@type']

                # Adicionar todas as outras propriedades do esquema, exceto '@context'
                schema.update({key: value for key, value in schema_data.items() if key != '@context'})

                # Adicionar o esquema à lista de esquemas
                schemas.append(schema)
        except Exception as e:
            print("Error parsing schema:", e)

    return schemas

async def crawl(url):
    visited = set()
    results = []
    queue = []  # Adicione a definição da variável queue
    internal_links = []  # Defina internal_links como uma lista vazia

    visited.add(url)
    print("Crawling:", url)

    async with aiohttp.ClientSession() as session:
        page_content = await get_page(url, session)
        if page_content:
            soup = BeautifulSoup(page_content, 'html.parser')

            metadata = await extract_metadata(soup)
            headings = await extract_headings(soup)
            internal_links, external_links = await extract_links(soup, url)  # Alteração aqui
            body = await extract_body(soup)
            favicon = await get_favicon(soup, url)
            sitemap = await check_sitemap(url)
            schemas = await extract_schema(soup)  # Extrair esquemas

            # Processar os links internos
            internal_links = internal_links or []  # Garante que internal_links seja uma lista
            for link in internal_links:
                if link not in visited:  # Verificar se o link já foi visitado
                    visited.add(link)
                    queue.append(link)  # Adicionar os links internos à fila

            result = {
                "crawlInformation": {
                    "firstCrawl": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "sitemap": sitemap if sitemap else ""  # Placeholder for sitemap information
                },
                "pageMetadata": metadata,
                "pageStructure": {
                    "headings": headings,
                    "deeplinks": "",  # Placeholder for deeplinks
                    "url": url,
                    "domain": urlparse(url).netloc,
                    "links": {
                        "external": external_links  # Alteração aqui
                    },
                    "body": body,
                    "schemas": schemas  # Adicionar esquemas extraídos
                }
            }

            results.append(result)

    return results, internal_links  # Retorna os resultados e os links internos

async def main():
    if len(sys.argv) != 2:
        url = input("Please enter the URL to crawl: ")
    else:
        url = sys.argv[1]

    visited = set()
    queue = [url]

    while queue:
        current_url = queue.pop(0)

        if current_url in visited:
            continue

        visited.add(current_url)
        print("Crawling:", current_url)

        # Check sitemap and add URLs to queue
        sitemap_urls = await check_sitemap(current_url)
        queue.extend(sitemap_urls)

        # Crawl the current URL
        results = await crawl(current_url)

        # Process results and continue crawling
        for result in results:
            # Send results to MeiliSearch
            # Extract internal links and add them to the queue
            for link in result['pageStructure']['links']:
                if link not in visited:
                    queue.append(link)

if __name__ == "__main__":
    asyncio.run(main())