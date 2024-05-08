from __future__ import annotations

import logging
import os

from flask import Flask, request

import sqlalchemy

from connect_connector import connect_with_connector

BUSINESSES = "businesses"
BUSINESS_ID = "business_id"
BUSINESSES_REQUIRED_ATTRIBUTES = ["owner_id", "name", "street_address", "city", "state", "zip_code"]
REVIEWS = "reviews"
REVIEW_ID = "review_id"
REVIEWS_REQUIRED_ATTRIBUTES = ["user_id", "business_id", "stars"]
POST_PUT_ERROR = {"Error" : "The request body is missing at least one of the required attributes"}


app = Flask(__name__)

logger = logging.getLogger()

def init_connection_pool() -> sqlalchemy.engine.base.Engine:
    if os.environ.get('INSTANCE_CONNECTION_NAME'):
        return connect_with_connector()
        
    raise ValueError(
        'Missing database connection type. Please define INSTANCE_CONNECTION_NAME'
    )

db = None

def init_db():
    global db
    db = init_connection_pool()


def create_table_businesses(db: sqlalchemy.engine.base.Engine) -> None:
    with db.connect() as conn:
        conn.execute(
            sqlalchemy.text(
                'CREATE TABLE IF NOT EXISTS businesses ('
                'business_id BIGINT NOT NULL AUTO_INCREMENT, '
                'owner_id INT NOT NULL,'
                'name VARCHAR(50) NOT NULL, '
                'street_address VARCHAR(100) NOT NULL, '
                'city VARCHAR(50) NOT NULL, '
                'state VARCHAR(2) NOT NULL, '
                'zip_code INT NOT NULL, '
                'PRIMARY KEY (business_id)'
                ');'
            )
        )
        conn.commit()


def create_table_reviews(db: sqlalchemy.engine.base.Engine) -> None:
    with db.connect() as conn:
        conn.execute(
            sqlalchemy.text(
                'CREATE TABLE IF NOT EXISTS reviews ('
                'review_id BIGINT NOT NULL AUTO_INCREMENT, '
                'user_id INT NOT NULL, '
                'business_id BIGINT NOT NULL, '
                'stars INT NOT NULL, '
                'review_text VARCHAR(1000), '
                'PRIMARY KEY (review_id), '
                'FOREIGN KEY (business_id) REFERENCES businesses(business_id) ON DELETE CASCADE'
                ');'
            )
        )
        conn.commit()


@app.route('/')
def index():
    return 'Please navigate to /lodgings to use this API'

def validate_business_post_put(request_json, attributes) -> bool:
    """Checks to see if the json in request has all required business attributes."""
    for key in attributes:
        if key not in request_json:
            return False
    return True

def generate_self_url(url, id) -> str:
    """Generates the url meant to be returned in the 'self' portion of the returned json dict."""
    return url +"/" + str(id)

#def generate_self_url(url, attribute, id) -> str:
#    """Generates the url meant to be returned in the 'self' portion of the returned json dict."""
#    return url + attribute + str(id)

@app.route('/' + BUSINESSES, methods=['POST'])
def post_business():
    """Create and return a business with the given request. If a required parameter is missing, 400 is returned"""
    content = request.get_json()
    valid_business = validate_business_post_put(content, BUSINESSES_REQUIRED_ATTRIBUTES)
    if not valid_business:
        return (POST_PUT_ERROR, 400)
    try:
        with db.connect() as conn:
            statement = sqlalchemy.text(
                'INSERT INTO businesses(owner_id, name, street_address, city, state, zip_code) '
                ' VALUES (:owner_id, :name, :street_address, :city, :state, :zip_code)'
            )
            conn.execute(statement, parameters={'owner_id': content['owner_id'], 
                                        'name': content['name'], 
                                        'street_address': content['street_address'], 
                                        'city': content['city'], 
                                        'state': content['state'], 
                                        'zip_code': content['zip_code']})
            statement2 = sqlalchemy.text('SELECT last_insert_id()')
            business_id = conn.execute(statement2).scalar()
            conn.commit()
    except Exception as e:
        logger.exception(e)
        return ({'Error': 'Unable to create business'}, 500)
    return ({'id': business_id,
             'owner_id': content['owner_id'], 
             'name': content['name'], 
             'street_address': content['street_address'], 
             'city': content['city'], 
             'state': content['state'], 
             'zip_code': content['zip_code'],
             'self': generate_self_url(request.base_url, business_id)}, 201)



@app.route('/' + BUSINESSES, methods=['GET'])
def get_businesses():
    offset = 0
    limit = 3
    url = request.url.split("/")
    # Check to see if final portion of URL is simply 'businesses'. If not, split the url further to get the limit and offset
    if url[-1] != BUSINESSES:
        url = url[11:]
        url = url.split("&")
        for param in url:
            if param[:6] == "offset":
                offset = int(param[-1])
            else:
                limit = int(param[-1])
    with db.connect() as conn:
        stmt = sqlalchemy.text(
                'SELECT * FROM businesses ORDER BY business_id LIMIT :limit OFFSET :offset'
            )
        businesses = []
        rows = conn.execute(stmt, parameters={'limit': limit, 'offset': offset})
        for row in rows:
            business = row._asdict()
            id = business["business_id"]
            business["self"] = generate_self_url(request.base_url, id)
            businesses.append(business)
        response_dict = {"entries": businesses}
        if len(business) == limit:
            offset += limit
            param_str = f"?offset={int(offset)}&limit={int(limit)}"
            next_url = request.url.split("?")
            next_url[-1] = param_str
            response_dict["next"] = "".join(next_url)

        return response_dict


def generate_not_found_message(business_or_review: str, id_attribute: str) -> dict:
    """Generates a generic error not found message for with the given type and attribute"""
    if business_or_review == "businesses":
        business_or_review = "business"
    else:
        business_or_review = "review"
    not_found_str = f"No {business_or_review} with this {id_attribute} exists"
    return {"Error": not_found_str}


@app.route('/' + BUSINESSES + '/<int:id>', methods=['GET'])
def get_business(id):
    """Gets and returns the business with a business_id corresponding to the given parameter id, or returns 404 if not found."""
    with db.connect() as conn:
        stmt = sqlalchemy.text(
                'SELECT business_id, owner_id, name, street_address, city, state, zip_code FROM businesses WHERE business_id=:business_id'
            )
        row = conn.execute(stmt, parameters={'business_id': id}).one_or_none()
        if row is None:
            return (generate_not_found_message(BUSINESSES, BUSINESS_ID), 404)
        else:
            business = row._asdict()
            business["self"] = request.base_url
            business["id"] = business["business_id"]
            del business["business_id"]
            return business


@app.route('/' + BUSINESSES + '/<int:id>', methods=['PUT'])
def put_business(id):
    """
    Edits a business with a business_id corresponding to the given parameter id. If not all required parameters are included
    in the request, a 400 error is returned. If no business_id corresponds to the given id, 404 is returned. Else, the updated
    business is returned along with a 200 status code.
    """
    content = request.get_json()
    valid_business = validate_business_post_put(content, BUSINESSES_REQUIRED_ATTRIBUTES)
    if not valid_business:
        return (POST_PUT_ERROR, 400)
    
    with db.connect() as conn:
        stmt = sqlalchemy.text(
                'SELECT business_id, owner_id, name, street_address, city, state, zip_code FROM businesses WHERE business_id=:business_id'
            )
        row = conn.execute(stmt, parameters={'business_id': id}).one_or_none()
        if row is None:
            return (generate_not_found_message(BUSINESSES, BUSINESS_ID), 404)
        else:
            stmt = sqlalchemy.text(
                'UPDATE businesses '
                'SET owner_id = :owner_id, name = :name, street_address = :street_address, city = :city, state = :state, zip_code = :zip_code '
                'WHERE business_id = :business_id'
            )
            conn.execute(stmt, parameters={'owner_id': content['owner_id'], 
                                           'name': content['name'], 
                                           'street_address': content['street_address'], 
                                           'city': content['city'], 
                                           'state': content['state'], 
                                           'zip_code': content['zip_code'],
                                           'business_id': id})
            conn.commit()
            return ({'id': id,
             'owner_id': content['owner_id'], 
             'name': content['name'], 
             'street_address': content['street_address'], 
             'city': content['city'], 
             'state': content['state'], 
             'zip_code': content['zip_code'],
             'self': request.base_url}, 200)


@app.route('/' + BUSINESSES + '/<int:id>', methods=['DELETE'])
def delete_business(id):
     """
     Delete a business with the business_id corresponding to the given parameter id. Also deletes all associated reviews.
     If no business corresponds to the given id, a 404 error message is returned.
     """
     with db.connect() as conn:
        stmt = sqlalchemy.text(
                'SELECT business_id, owner_id, name, street_address, city, state, zip_code FROM businesses WHERE business_id=:business_id'
            )
        row = conn.execute(stmt, parameters={'business_id': id}).one_or_none()
        if row is None:
            return (generate_not_found_message(BUSINESSES, BUSINESS_ID), 404)
        
        stmt = sqlalchemy.text(
                'DELETE FROM businesses WHERE business_id=:business_id'
            )
        conn.execute(stmt, parameters={'business_id': id})
        conn.commit()
        return ('', 204)


@app.route('/owners' + '/<int:id>/' + BUSINESSES, methods=['GET'])
def get_owners_businesses(id):
    """Return all businesses associated with the owner with the given id"""
    with db.connect() as conn:
        stmt = sqlalchemy.text(
                'SELECT * FROM businesses WHERE owner_id=:owner_id'
            )
        rows = conn.execute(stmt, parameters={'owner_id': id})
        business_list = list()
        for row in rows:
            business = row._asdict()
            business["self"] = generate_self_url(request.base_url, id)
            business_list.append(business)
        return business_list


@app.route('/' + REVIEWS, methods=['POST'])
def post_review():
    """Create and return a review with the given request. If a required parameter is missing, 400 is returned"""
    content = request.get_json()
    valid_review = validate_business_post_put(content, REVIEWS_REQUIRED_ATTRIBUTES)
    has_review_text = False
    if "review_text" in content:
        has_review_text = True
    if not valid_review:
        return (POST_PUT_ERROR, 400)
    try:
        with db.connect() as conn:
            id = content["business_id"]
            # Check if business with given id exists
            stmt = sqlalchemy.text(
                'SELECT business_id, owner_id, name, street_address, city, state, zip_code FROM businesses WHERE business_id=:business_id'
            )
            row = conn.execute(stmt, parameters={'business_id': id}).one_or_none()
            if row is None:
                return (generate_not_found_message(BUSINESSES, BUSINESS_ID), 404)
            # Check if user has already left a reivew for this business
            stmt = sqlalchemy.text(
                'SELECT * FROM reviews WHERE business_id=:business_id'
            )
            row = conn.execute(stmt, parameters={'business_id': id}).one_or_none()
            if row is not None:
                review = row._asdict()
                if review["user_id"] == content["user_id"]:
                    error_message = {"Error":  "You have already submitted a review for this business. You can update your previous review, or delete it and submit a new review"} 
                    return (error_message, 409)
            
            if has_review_text:
                statement = sqlalchemy.text(
                'INSERT INTO reviews(user_id, business_id, stars, review_text) '
                ' VALUES (:user_id, :business_id, :stars, :review_text)')
                conn.execute(statement, parameters={'user_id': content['user_id'], 
                                        'business_id': content['business_id'], 
                                        'stars': content['stars'], 
                                        'review_text': content['review_text']})
            else:
                statement = sqlalchemy.text(
                'INSERT INTO reviews(user_id, business_id, stars) '
                ' VALUES (:user_id, :business_id, :stars)')
                conn.execute(statement, parameters={'user_id': content['user_id'], 
                                        'business_id': content['business_id'], 
                                        'stars': content['stars']})
            
            statement2 = sqlalchemy.text('SELECT last_insert_id()')
            review_id = conn.execute(statement2).scalar()
            conn.commit()
    except Exception as e:
        logger.exception(e)
        return ({'Error': 'Unable to create review'}, 500)
    if has_review_text:
        return ({'id': review_id,
             'user_id': content['user_id'], 
             'business_id': content['business_id'], 
             'stars': content['stars'], 
             'review_text': content['review_text'], 
             'self': generate_self_url(request.base_url, review_id)}, 201)
    return ({'id': review_id,
             'user_id': content['user_id'], 
             'business_id': content['business_id'], 
             'stars': content['stars'], 
             'self': generate_self_url(request.base_url, review_id)}, 201)


@app.route('/' + REVIEWS + '/<int:id>', methods=['GET'])
def get_review(id):
    """Gets and returns the review with a review_id corresponding to the given parameter id, or returns 404 if not found."""
    with db.connect() as conn:
        stmt = sqlalchemy.text(
                'SELECT * FROM reviews WHERE review_id=:review_id'
            )
        row = conn.execute(stmt, parameters={'review_id': id}).one_or_none()
        if row is None:
            return (generate_not_found_message(REVIEWS, REVIEW_ID), 404)
        else:
            review = row._asdict()
            review["self"] = request.base_url
            return review
        

@app.route('/' + REVIEWS + '/<int:id>', methods=['PUT'])
def put_review(id):
    """
    Edits a review with a review_id corresponding to the given parameter id. If not all required parameters are included
    in the request, a 400 error is returned. If no review_id corresponds to the given id, 404 is returned. Else, the updated
    review is returned along with a 200 status code.
    """
    content = request.get_json()
    if "stars" not in content:
        return (POST_PUT_ERROR, 400)
    
    has_review_text = False
    if "review_text" in content:
        has_review_text = True

    with db.connect() as conn:
        stmt = sqlalchemy.text(
                'SELECT * FROM reviews WHERE review_id=:review_id'
            )
        row = conn.execute(stmt, parameters={'review_id': id}).one_or_none()
        if row is None:
            return (generate_not_found_message(REVIEWS, REVIEW_ID), 404)
        else:
            if has_review_text:
                stmt = sqlalchemy.text(
                'UPDATE reviews '
                'SET stars = :stars, review_text = :review_text '
                'WHERE review_id = :review_id')
                conn.execute(stmt, parameters={'stars': content['stars'], 
                                           'review_text': content['review_text'],
                                           'review_id': id})
            else:
                stmt = sqlalchemy.text(
                'UPDATE reviews '
                'SET stars = :stars '
                'WHERE review_id = :review_id')
                conn.execute(stmt, parameters={'stars': content['stars'], 'review_id': id})
            conn.commit()
            
            row = row._asdict()
            row["stars"] = content["stars"]
            business_id = row["business_id"]
            row["business"] = generate_self_url(request.base_url, business_id)
            del row["business_id"]
            if has_review_text:
                row["review_text"] = content["review_text"]
            row["self"] = generate_self_url(request.base_url, id)
            return row, 200
        

@app.route('/' + REVIEWS + '/<int:id>', methods=['DELETE'])
def delete_review(id):
     """
     Delete a review with the review_id corresponding to the given parameter id.
     If no reivew corresponds to the given id, a 404 error message is returned.
     """
     with db.connect() as conn:
        stmt = sqlalchemy.text(
                'SELECT * FROM reviews WHERE review_id=:review_id'
            )
        row = conn.execute(stmt, parameters={'review_id': id}).one_or_none()
        if row is None:
            return (generate_not_found_message(REVIEWS, REVIEW_ID), 404)
        
        stmt = sqlalchemy.text(
                'DELETE FROM reviews WHERE review_id=:review_id'
            )
        conn.execute(stmt, parameters={'review_id': id})
        conn.commit()
        return ('', 204)


@app.route('/users' + '/<int:id>/' + REVIEWS, methods=['GET'])
def get_users_reviews(id):
    """Return all reviews associated with the user with the given id"""
    with db.connect() as conn:
        stmt = sqlalchemy.text(
                'SELECT * FROM reviews WHERE user_id=:user_id'
            )
        rows = conn.execute(stmt, parameters={'user_id': id})
        review_list = list()
        for row in rows:
            review = row._asdict()
            url = request.base_url.split("users")
            url[-1] = REVIEWS + "/" + str(review["review_id"])
            review["self"] = "".join(url)
            business_id = review["business_id"]
            url = request.base_url.split("users")
            url[-1] = BUSINESSES + "/" + str(business_id)
            review["business"] = "".join(url)
            del review["business_id"]
            review["id"] = review["review_id"]
            del review["review_id"]
            review_list.append(review)
        return review_list
    

if __name__ == '__main__':
    init_db()
    create_table_businesses(db)
    create_table_reviews(db)
    app.run(host='0.0.0.0', port=8080, debug=True)
